package com.ramya.s3upload;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * Orchestrates the streaming multipart upload of data to S3.
 *
 * <p>This is the main entry point. Configure it with builder-style methods,
 * call {@link #getMultiPartOutputStreams()} to get output streams to write to,
 * write your data, close the streams, then call {@link #complete()}.</p>
 *
 * <h3>Retry with Exponential Backoff</h3>
 * <p>Unlike a naive implementation that aborts the entire upload on any part failure,
 * this manager retries individual part uploads with exponential backoff. Transient
 * S3 errors (5xx, throttling, network issues) are retried up to {@code maxRetries}
 * times with increasing delays ({@code baseRetryDelayMs * 2^attempt}), allowing
 * the upload to survive temporary infrastructure hiccups without losing progress.</p>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
 *
 * StreamTransferManager manager = new StreamTransferManager("my-bucket", "my-key", s3Client)
 *     .numStreams(2)
 *     .numUploadThreads(2)
 *     .queueCapacity(2)
 *     .partSize(10)
 *     .maxRetries(3)
 *     .baseRetryDelayMs(1000);
 *
 * List<MultiPartOutputStream> streams = manager.getMultiPartOutputStreams();
 *
 * // Write data to streams (can be done in parallel threads)
 * streams.get(0).write(data1);
 * streams.get(1).write(data2);
 *
 * // Close all streams when done writing
 * for (MultiPartOutputStream stream : streams) {
 *     stream.close();
 * }
 *
 * // Complete the upload
 * manager.complete();
 * }</pre>
 */
public class StreamTransferManager {

    private static final Logger log = LoggerFactory.getLogger(StreamTransferManager.class);

    private static final int MAX_PART_NUMBER = 10_000;
    private static final long MB = 1024 * 1024;

    private final String bucketName;
    private final String putKey;
    private final AmazonS3 s3Client;

    // Configuration (set before getMultiPartOutputStreams)
    private int numStreams = 1;
    private int numUploadThreads = 1;
    private int queueCapacity = 1;
    private long partSize = 5 * MB;
    private boolean checkIntegrity = false;

    // Retry configuration
    private int maxRetries = 3;
    private long baseRetryDelayMs = 1000;

    // Runtime state
    private String uploadId;
    private ClosableQueue<StreamPart> queue;
    private ExecutorServiceResultsHandler<Void> executorServiceResultsHandler;
    private final List<PartETag> partETags = Collections.synchronizedList(new ArrayList<PartETag>());
    private StreamPart leftoverStreamPart;
    private final Object leftoverLock = new Object();
    private int finishedCount;
    private volatile boolean isAborting = false;
    private boolean started = false;

    public StreamTransferManager(String bucketName, String putKey, AmazonS3 s3Client) {
        this.bucketName = bucketName;
        this.putKey = putKey;
        this.s3Client = s3Client;
    }

    // ---- Builder-style configuration ----

    public StreamTransferManager numStreams(int numStreams) {
        ensureNotStarted();
        this.numStreams = numStreams;
        return this;
    }

    public StreamTransferManager numUploadThreads(int numUploadThreads) {
        ensureNotStarted();
        this.numUploadThreads = numUploadThreads;
        return this;
    }

    public StreamTransferManager queueCapacity(int queueCapacity) {
        ensureNotStarted();
        this.queueCapacity = queueCapacity;
        return this;
    }

    /**
     * Sets the target part size in megabytes. Minimum 5 MB (S3 requirement).
     */
    public StreamTransferManager partSize(long partSizeMB) {
        ensureNotStarted();
        this.partSize = partSizeMB * MB;
        return this;
    }

    public StreamTransferManager checkIntegrity(boolean checkIntegrity) {
        ensureNotStarted();
        this.checkIntegrity = checkIntegrity;
        return this;
    }

    /**
     * Sets the maximum number of retry attempts for a single part upload.
     * Default is 3. Set to 0 to disable retries.
     */
    public StreamTransferManager maxRetries(int maxRetries) {
        ensureNotStarted();
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * Sets the base delay in milliseconds for retry backoff.
     * Actual delay = baseRetryDelayMs * 2^attemptNumber.
     * Default is 1000ms (so delays are 1s, 2s, 4s for 3 retries).
     */
    public StreamTransferManager baseRetryDelayMs(long baseRetryDelayMs) {
        ensureNotStarted();
        this.baseRetryDelayMs = baseRetryDelayMs;
        return this;
    }

    private void ensureNotStarted() {
        if (started) {
            throw new IllegalStateException(
                    "Configuration cannot be changed after getMultiPartOutputStreams() has been called");
        }
    }

    // ---- Customization hooks (override in subclass) ----

    protected void customiseInitiateRequest(InitiateMultipartUploadRequest request) {
    }

    protected void customiseUploadPartRequest(UploadPartRequest request) {
    }

    protected void customiseCompleteRequest(CompleteMultipartUploadRequest request) {
    }

    protected void customisePutEmptyObjectRequest(PutObjectRequest request) {
    }

    // ---- Main API ----

    /**
     * Initiates the multipart upload and returns output streams to write data to.
     * After writing, close all streams and call {@link #complete()}.
     */
    public List<MultiPartOutputStream> getMultiPartOutputStreams() {
        started = true;

        queue = new ClosableQueue<>(queueCapacity);

        InitiateMultipartUploadRequest initRequest =
                new InitiateMultipartUploadRequest(bucketName, putKey);
        customiseInitiateRequest(initRequest);

        InitiateMultipartUploadResult initResult = s3Client.initiateMultipartUpload(initRequest);
        uploadId = initResult.getUploadId();
        log.info("Initiated multipart upload to s3://{}/{} (uploadId={})",
                bucketName, putKey, Utils.skipMiddle(uploadId, 20));

        // Divide part numbers among streams
        int partNumbersPerStream = MAX_PART_NUMBER / numStreams;
        List<MultiPartOutputStream> streams = new ArrayList<>();
        for (int i = 0; i < numStreams; i++) {
            int start = i * partNumbersPerStream + 1;
            int end = (i + 1) * partNumbersPerStream;
            streams.add(new MultiPartOutputStream(start, end, partSize, queue));
        }

        // Start upload threads
        executorServiceResultsHandler = new ExecutorServiceResultsHandler<>(
                Executors.newFixedThreadPool(numUploadThreads));

        for (int i = 0; i < numUploadThreads; i++) {
            executorServiceResultsHandler.submit(new UploadTask());
        }
        executorServiceResultsHandler.finishedSubmitting();

        return streams;
    }

    /**
     * Completes the multipart upload. Call this after closing all output streams.
     * Blocks until all upload threads have finished.
     */
    public void complete() {
        try {
            // Wait for all upload threads to finish
            executorServiceResultsHandler.awaitCompletion();

            // Upload any remaining leftover part
            synchronized (leftoverLock) {
                if (leftoverStreamPart != null) {
                    log.info("Uploading final leftover part #{} ({} bytes)",
                            leftoverStreamPart.getPartNumber(), leftoverStreamPart.size());
                    uploadStreamPartWithRetry(leftoverStreamPart);
                    leftoverStreamPart = null;
                }
            }

            if (partETags.isEmpty()) {
                // Nothing was written — abort multipart and do a regular empty put
                log.info("No data written. Aborting multipart upload and putting empty object.");
                s3Client.abortMultipartUpload(
                        new AbortMultipartUploadRequest(bucketName, putKey, uploadId));

                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(0);
                PutObjectRequest putRequest = new PutObjectRequest(
                        bucketName, putKey, new ByteArrayInputStream(new byte[0]), metadata);
                customisePutEmptyObjectRequest(putRequest);
                s3Client.putObject(putRequest);
                return;
            }

            // Sort ETags by part number (required by S3)
            Collections.sort(partETags, new Comparator<PartETag>() {
                @Override
                public int compare(PartETag a, PartETag b) {
                    return Integer.compare(a.getPartNumber(), b.getPartNumber());
                }
            });

            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                    bucketName, putKey, uploadId, partETags);
            customiseCompleteRequest(completeRequest);

            String actualETag = s3Client.completeMultipartUpload(completeRequest).getETag();
            log.info("Completed multipart upload. ETag: {}", actualETag);

            // Optional integrity check
            if (checkIntegrity) {
                String expectedETag = computeExpectedETag(partETags);
                if (!expectedETag.equals(actualETag)) {
                    throw new IntegrityCheckException(expectedETag, actualETag);
                }
                log.info("Integrity check passed.");
            }

        } catch (IntegrityCheckException e) {
            throw e;
        } catch (RuntimeException e) {
            abort();
            throw e;
        }
    }

    /**
     * Aborts the multipart upload. Shuts down threads, closes the queue,
     * and tells S3 to discard uploaded parts.
     */
    public void abort() {
        if (isAborting) {
            return;
        }
        isAborting = true;

        log.warn("Aborting multipart upload for s3://{}/{}", bucketName, putKey);

        if (executorServiceResultsHandler != null) {
            executorServiceResultsHandler.abort();
        }

        if (queue != null) {
            queue.close();
        }

        if (uploadId != null) {
            try {
                s3Client.abortMultipartUpload(
                        new AbortMultipartUploadRequest(bucketName, putKey, uploadId));
                log.info("Aborted multipart upload (uploadId={})", Utils.skipMiddle(uploadId, 20));
            } catch (Exception e) {
                log.error("Failed to abort multipart upload", e);
            }
        }
    }

    // ---- Part upload with retry + exponential backoff ----

    /**
     * Uploads a single part to S3 with retry and exponential backoff.
     *
     * <p>If the upload fails with a retryable error (5xx server errors,
     * throttling, or transient network issues), it will be retried up to
     * {@code maxRetries} times. The delay between retries grows exponentially:
     * {@code baseRetryDelayMs * 2^attempt} (e.g., 1s, 2s, 4s with defaults).</p>
     *
     * <p>Non-retryable errors (4xx client errors like access denied, invalid bucket)
     * are thrown immediately without retry.</p>
     *
     * @param part the StreamPart to upload
     * @throws RuntimeException if all retry attempts are exhausted
     */
    private void uploadStreamPartWithRetry(StreamPart part) {
        Exception lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 0) {
                    long delay = baseRetryDelayMs * (1L << (attempt - 1)); // exponential: 1s, 2s, 4s...
                    log.warn("Retry {}/{} for part #{} after {}ms delay",
                            attempt, maxRetries, part.getPartNumber(), delay);
                    Thread.sleep(delay);
                }

                UploadPartRequest request = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(putKey)
                        .withUploadId(uploadId)
                        .withPartNumber(part.getPartNumber())
                        .withInputStream(part.getInputStream())
                        .withPartSize(part.size());

                if (checkIntegrity) {
                    request.setMd5Digest(part.getMD5Digest());
                }

                customiseUploadPartRequest(request);

                UploadPartResult result = s3Client.uploadPart(request);
                partETags.add(result.getPartETag());

                if (attempt > 0) {
                    log.info("Part #{} succeeded on retry {}", part.getPartNumber(), attempt);
                } else {
                    log.debug("Uploaded part #{} ({} bytes)", part.getPartNumber(), part.size());
                }
                return; // success

            } catch (AmazonServiceException e) {
                lastException = e;

                if (!isRetryable(e)) {
                    log.error("Non-retryable error uploading part #{}: {} (status {})",
                            part.getPartNumber(), e.getMessage(), e.getStatusCode());
                    throw new RuntimeException("Non-retryable S3 error for part #"
                            + part.getPartNumber() + ": " + e.getMessage(), e);
                }

                log.warn("Retryable error uploading part #{}: {} (status {}, error code: {})",
                        part.getPartNumber(), e.getMessage(), e.getStatusCode(),
                        e.getErrorCode());

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while retrying part #"
                        + part.getPartNumber(), e);

            } catch (Exception e) {
                // Network errors, timeouts, etc. are retryable
                lastException = e;
                log.warn("Error uploading part #{}: {}", part.getPartNumber(), e.getMessage());
            }
        }

        // All retries exhausted
        log.error("All {} retries exhausted for part #{}",
                maxRetries, part.getPartNumber());
        throw new RuntimeException("Failed to upload part #" + part.getPartNumber()
                + " after " + (maxRetries + 1) + " attempts", lastException);
    }

    /**
     * Determines whether an S3 error is retryable.
     * 5xx server errors and 429 throttling are retryable.
     * 4xx client errors (bad request, access denied, etc.) are not.
     */
    private boolean isRetryable(AmazonServiceException e) {
        int status = e.getStatusCode();
        // 5xx = server error (retryable), 429 = throttled (retryable)
        if (status >= 500 || status == 429) {
            return true;
        }
        // 408 Request Timeout is retryable
        if (status == 408) {
            return true;
        }
        return false;
    }

    // ---- Upload consumer task ----

    private class UploadTask implements Callable<Void> {

        @Override
        public Void call() {
            while (true) {
                StreamPart part;

                synchronized (queue) {
                    if (finishedCount >= numStreams) {
                        break; // all producer streams are done
                    }
                    try {
                        part = queue.take();
                    } catch (InterruptedException e) {
                        throw Utils.runtimeInterruptedException(e);
                    }

                    if (part == StreamPart.POISON) {
                        finishedCount++;
                        continue;
                    }
                }

                // Handle undersized parts (< 5MB) by merging into leftover
                if (part.size() < MultiPartOutputStream.S3_MIN_PART_SIZE) {
                    synchronized (leftoverLock) {
                        if (leftoverStreamPart == null) {
                            leftoverStreamPart = part;
                        } else {
                            // Merge: append higher-numbered part into lower-numbered one
                            StreamPart lower, higher;
                            if (leftoverStreamPart.getPartNumber() < part.getPartNumber()) {
                                lower = leftoverStreamPart;
                                higher = part;
                            } else {
                                lower = part;
                                higher = leftoverStreamPart;
                            }
                            lower.getOutputStream().append(higher.getOutputStream());
                            leftoverStreamPart = lower;

                            // If merged leftover is now big enough, upload it
                            if (leftoverStreamPart.size() >= MultiPartOutputStream.S3_MIN_PART_SIZE) {
                                uploadStreamPartWithRetry(leftoverStreamPart);
                                leftoverStreamPart = null;
                            }
                        }
                    }
                } else {
                    uploadStreamPartWithRetry(part);
                }
            }
            return null;
        }
    }

    // ---- ETag computation for integrity check ----

    private String computeExpectedETag(List<PartETag> tags) {
        MessageDigest md5 = Utils.md5();
        for (PartETag tag : tags) {
            md5.update(hexStringToBytes(tag.getETag()));
        }
        String hex = bytesToHex(md5.digest());
        return hex + "-" + tags.size();
    }

    private static byte[] hexStringToBytes(String hex) {
        hex = hex.replace("\"", ""); // S3 ETags sometimes have quotes
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }
}
