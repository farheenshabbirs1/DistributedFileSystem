package com.farheenshaikh.dfs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** SHA-256 checksums, used across the cluster for corruption detection. */
public final class ChecksumUtil {

    private ChecksumUtil() {
    }

    public static String sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed to be available on every standard JVM; this would only fire
            // on a broken or non-standard installation.
            throw new IllegalStateException("SHA-256 is not available on this JVM", e);
        }
    }
}
