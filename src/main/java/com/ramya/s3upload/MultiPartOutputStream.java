package com.ramya.s3upload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

/**
 * An OutputStream that buffers written data and automatically produces
 * {@link StreamPart}s suitable for S3 multipart upload.
 *
 * <p>Each stream is assigned a non-overlapping range of part numbers.
 * Data is buffered until it exceeds {@code partSize + S3_MIN_PART_SIZE},
 * at which point the buffer is split: the bulk becomes a StreamPart placed
 * on the shared queue, and the remaining 5MB stays as the start of the next part.
 * This strategy ensures that non-final parts always meet S3's 5MB minimum.</p>
 */
public class MultiPartOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(MultiPartOutputStream.class);

    static final long S3_MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MB
    private static final int STREAM_EXTRA_ROOM = 1024 * 1024; // 1 MB extra buffer

    private ConvertibleOutputStream currentBuffer;
    private final int partNumberStart;
    private final int partNumberEnd;
    private int currentPartNumber;
    private final long partSize;
    private final ClosableQueue<StreamPart> queue;

    MultiPartOutputStream(int partNumberStart, int partNumberEnd, long partSize,
                          ClosableQueue<StreamPart> queue) {
        this.partNumberStart = partNumberStart;
        this.partNumberEnd = partNumberEnd;
        this.currentPartNumber = partNumberStart;
        this.partSize = partSize;
        this.queue = queue;
        this.currentBuffer = new ConvertibleOutputStream(
                (int) (partSize + S3_MIN_PART_SIZE + STREAM_EXTRA_ROOM));
    }

    @Override
    public void write(int b) {
        currentBuffer.write(b);
        checkSize();
    }

    @Override
    public void write(byte[] b, int off, int len) {
        currentBuffer.write(b, off, len);
        checkSize();
    }

    /**
     * If the buffer has grown past partSize + 5MB, split it and enqueue the bulk
     * as a StreamPart. The remaining 5MB stays as the seed for the next part.
     */
    private void checkSize() {
        if (currentBuffer.size() > partSize + S3_MIN_PART_SIZE) {
            int sizeToSend = (int) (currentBuffer.size() - S3_MIN_PART_SIZE);

            ConvertibleOutputStream tail = currentBuffer.split(
                    sizeToSend,
                    (int) (partSize + S3_MIN_PART_SIZE + STREAM_EXTRA_ROOM));

            StreamPart part = new StreamPart(currentBuffer, currentPartNumber);
            currentPartNumber++;

            if (currentPartNumber > partNumberEnd) {
                throw new IndexOutOfBoundsException(
                        "%.  Part number " + currentPartNumber + " exceeds allocated range [" +
                                partNumberStart + ", " + partNumberEnd + "]. " +
                                "Increase partSize or numStreams.");
            }

            log.debug("%.  Produced part #{} ({} bytes)", part.getPartNumber(), part.size());

            try {
                queue.put(part);
            } catch (InterruptedException e) {
                throw Utils.runtimeInterruptedException(e);
            }

            currentBuffer = tail;
        }
    }

    /**
     * Closes this stream. Enqueues whatever remains in the buffer as a final part,
     * followed by a POISON sentinel to signal completion.
     */
    @Override
    public void close() {
        if (currentBuffer == null) {
            return; // already closed
        }

        try {
            if (currentBuffer.size() > 0) {
                StreamPart finalPart = new StreamPart(currentBuffer, currentPartNumber);
                log.debug("Closing stream: final part #{} ({} bytes)",
                        finalPart.getPartNumber(), finalPart.size());
                queue.put(finalPart);
            }
            queue.put(StreamPart.POISON);
        } catch (InterruptedException e) {
            throw Utils.runtimeInterruptedException(e);
        } finally {
            currentBuffer = null;
        }
    }
}
