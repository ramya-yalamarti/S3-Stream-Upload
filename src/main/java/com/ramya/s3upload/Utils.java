package com.ramya.s3upload;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Internal utility methods.
 */
class Utils {

    private Utils() {
    }

    /**
     * Creates a new MD5 MessageDigest instance.
     */
    static MessageDigest md5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    /**
     * Converts a checked InterruptedException into an unchecked RuntimeException,
     * preserving the thread's interrupted status.
     */
    static RuntimeException runtimeInterruptedException(InterruptedException e) {
        Thread.currentThread().interrupt();
        return new RuntimeException(e);
    }

    /**
     * Truncates a string by replacing the middle with "..." if it exceeds the given length.
     */
    static String skipMiddle(String s, int maxLength) {
        if (s.length() <= maxLength) {
            return s;
        }
        int half = (maxLength - 3) / 2;
        return s.substring(0, half) + "..." + s.substring(s.length() - half);
    }
}
