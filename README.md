# S3 Stream Upload

Stream data of unknown size to AWS S3 using multipart upload — without buffering the entire object in memory or writing to disk.

## The Problem

S3's standard PutObject API requires you to specify `Content-Length` upfront. If you're generating data on the fly (log aggregation, ETL pipelines, report generation), you'd have to buffer everything in memory or write to a temp file first. Neither scales well.

## The Solution

This library uses S3's **multipart upload API** behind a standard Java `OutputStream` interface. You write bytes, it automatically chunks them into parts and uploads them in parallel. When you're done, it assembles the final S3 object.

### Key Feature: Retry with Exponential Backoff

Unlike naive implementations that abort the entire upload when a single part fails, this library **retries individual parts** with exponential backoff:

- Transient errors (5xx, 429 throttling, timeouts) are retried automatically
- Non-retryable errors (4xx client errors) fail immediately
- Configurable max retries and base delay
- Default: 3 retries with delays of 1s, 2s, 4s

This means a temporary S3 hiccup doesn't waste the work already done uploading other parts.

## Usage

```java
AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

StreamTransferManager manager = new StreamTransferManager("my-bucket", "data/output.gz", s3Client)
    .numStreams(2)          // parallel producer streams
    .numUploadThreads(2)    // parallel upload threads
    .queueCapacity(2)       // back-pressure buffer
    .partSize(10)           // MB per part
    .maxRetries(3)          // retries per part on failure
    .baseRetryDelayMs(1000) // 1s base delay (doubles each retry)
    .checkIntegrity(true);  // verify ETags after completion

List<MultiPartOutputStream> streams = manager.getMultiPartOutputStreams();

// Write data (can use multiple streams from different threads)
try {
    OutputStream out = streams.get(0);
    out.write(generateData());
    out.close();
} catch (Exception e) {
    manager.abort();
    throw e;
}

manager.complete();
```

## Architecture

```
Your Code                    Library                              AWS S3
─────────                    ───────                              ──────

write(bytes) ──────> MultiPartOutputStream
                        │ buffers until partSize + 5MB
                        │ splits buffer
                        ▼
                     StreamPart ──────> ClosableQueue (back-pressure)
                                           │
                                           ▼
                                      UploadTask thread(s)
                                           │
                                           ├─ size >= 5MB ──> uploadWithRetry() ──> S3
                                           │                     │ on failure:
                                           │                     │ retry 1 → wait 1s
                                           │                     │ retry 2 → wait 2s
                                           │                     │ retry 3 → wait 4s
                                           │                     │ give up → abort
                                           │
                                           └─ size < 5MB ──> merge into leftover

complete() ──────> upload leftover ──> CompleteMultipartUpload ──> S3
```

### Classes

| Class | Role |
|-------|------|
| `StreamTransferManager` | Orchestrator — initiates upload, manages threads, completes/aborts |
| `MultiPartOutputStream` | Producer — OutputStream that auto-chunks data into parts |
| `StreamPart` | Data unit — a chunk with part number, ready for upload |
| `ConvertibleOutputStream` | Smart buffer — split, append, zero-copy InputStream conversion |
| `ClosableQueue` | Thread-safe queue that can be closed on abort |
| `ExecutorServiceResultsHandler` | Thread pool wrapper with clean error propagation |
| `Utils` | MD5, interrupt handling, string utilities |
| `IntegrityCheckException` | Thrown on ETag mismatch after upload |

## Design Decisions

**5MB minimum handling**: S3 requires all parts except the last to be >= 5MB. Handled at two levels:
1. `MultiPartOutputStream` buffers to `partSize + 5MB` before splitting, so leftovers are always >= 5MB
2. `StreamTransferManager` merges any sub-5MB parts that still occur (when a stream writes very little data)

**Zero-copy buffers**: `ConvertibleOutputStream.toInputStream()` shares the underlying byte array — no copying when handing data to the S3 SDK.

**Back-pressure**: When the queue is full, `write()` blocks. Memory stays bounded at:
```
(numUploadThreads + queueCapacity) * partSize + numStreams * (partSize + 6MB)
```

**Clean abort**: On any failure, the queue is closed (unblocking producers), threads are interrupted, and `AbortMultipartUploadRequest` cleans up S3-side state.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `numStreams` | 1 | Number of output streams for parallel writing |
| `numUploadThreads` | 1 | Number of threads uploading parts to S3 |
| `queueCapacity` | 1 | Bounded queue size between producers and uploaders |
| `partSize` | 5 (MB) | Target size of each upload part |
| `checkIntegrity` | false | Verify composite ETag after completion |
| `maxRetries` | 3 | Max retry attempts per part on transient failure |
| `baseRetryDelayMs` | 1000 | Base delay in ms (doubles each retry) |

## Requirements

- Java 8+
- AWS SDK for Java v1 (`aws-java-sdk-s3`)

## Building

```bash
mvn clean package
```

## Inspiration

Inspired by [alexmojaki/s3-stream-upload](https://github.com/alexmojaki/s3-stream-upload), with the addition of per-part retry with exponential backoff for resilient uploads.

## License

MIT
