package com.ramya.s3upload;

/**
 * Thrown when the ETag returned by S3 after completing a multipart upload
 * does not match the expected composite ETag computed from individual part MD5s.
 * Note: This is thrown after the upload has already completed on S3's side.
 */
public class IntegrityCheckException extends RuntimeException {

    public IntegrityCheckException(String expectedETag, String actualETag) {
        super("Integrity check failed. Expected ETag: " + expectedETag
                + ", but S3 returned: " + actualETag);
    }
}
