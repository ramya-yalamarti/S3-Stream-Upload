package com.ramya.s3upload;

import java.io.InputStream;
import java.util.Base64;

/**
 * Represents a single chunk of data ready to be uploaded as one part
 * of an S3 multipart upload.
 */
class StreamPart {

    /**
     * Poison pill sentinel placed on the queue to signal that a producer stream has closed.
     */
    static final StreamPart POISON = new StreamPart(null, -1);

    private final ConvertibleOutputStream stream;
    private final int partNumber;

    StreamPart(ConvertibleOutputStream stream, int partNumber) {
        this.stream = stream;
        this.partNumber = partNumber;
    }

    int getPartNumber() {
        return partNumber;
    }

    int size() {
        return stream.size();
    }

    /**
     * Returns an InputStream over this part's data (zero-copy).
     */
    InputStream getInputStream() {
        return stream.toInputStream();
    }

    /**
     * Returns the Base64-encoded MD5 digest suitable for the Content-MD5 header.
     */
    String getMD5Digest() {
        return Base64.getEncoder().encodeToString(stream.getMD5Digest());
    }

    /**
     * Returns the raw MD5 digest bytes for composite ETag computation.
     */
    byte[] getRawMD5() {
        return stream.getMD5Digest();
    }

    ConvertibleOutputStream getOutputStream() {
        return stream;
    }
}
