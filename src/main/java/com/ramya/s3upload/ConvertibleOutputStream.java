package com.ramya.s3upload;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

/**
 * A ByteArrayOutputStream that supports zero-copy conversion to InputStream,
 * splitting its buffer at a given position, and appending from another stream.
 */
class ConvertibleOutputStream extends ByteArrayOutputStream {

    ConvertibleOutputStream() {
        super();
    }

    ConvertibleOutputStream(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Returns an InputStream backed by the same underlying byte array (zero copy).
     * The returned stream is only valid as long as this output stream is not modified.
     */
    InputStream toInputStream() {
        return new ByteArrayInputStream(buf, 0, count);
    }

    /**
     * Splits this stream at the given position.
     * This stream is truncated to contain only the first {@code countToKeep} bytes.
     * The remaining bytes are moved into a new ConvertibleOutputStream which is returned.
     *
     * @param countToKeep           number of bytes to keep in this stream
     * @param newStreamInitCapacity initial capacity for the new stream holding the tail bytes
     * @return a new stream containing bytes from position countToKeep to the end
     */
    ConvertibleOutputStream split(int countToKeep, int newStreamInitCapacity) {
        ConvertibleOutputStream tail = new ConvertibleOutputStream(newStreamInitCapacity);
        tail.write(buf, countToKeep, count - countToKeep);
        count = countToKeep;
        return tail;
    }

    /**
     * Appends all bytes from the other stream into this one.
     */
    void append(ConvertibleOutputStream other) {
        try {
            other.writeTo(this);
        } catch (IOException e) {
            // ByteArrayOutputStream.writeTo should never throw for in-memory streams
            throw new RuntimeException(e);
        }
    }

    /**
     * Computes the MD5 digest of the current buffer contents.
     */
    byte[] getMD5Digest() {
        MessageDigest md5 = Utils.md5();
        md5.update(buf, 0, count);
        return md5.digest();
    }
}
