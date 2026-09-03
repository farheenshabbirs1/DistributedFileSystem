package com.farheenshaikh.dfs;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A single storage node in the cluster: holds chunk bytes plus a checksum for
 * each chunk, and reports liveness via periodic heartbeats.
 */
public class StorageNode {

    public final String id;
    private final Map<String, byte[]> chunks = new ConcurrentHashMap<>();
    private final Map<String, String> checksums = new ConcurrentHashMap<>();
    private volatile long lastHeartbeat = System.currentTimeMillis();

    public StorageNode(String id) {
        this.id = id;
    }

    public void store(String chunkId, byte[] data) {
        chunks.put(chunkId, data.clone());
        checksums.put(chunkId, ChecksumUtil.sha256(data));
    }

    /** Reads a chunk, verifying it against its stored checksum. Throws on corruption. */
    public byte[] read(String chunkId) throws IOException {
        byte[] data = chunks.get(chunkId);
        if (data == null) {
            return null;
        }
        if (!ChecksumUtil.sha256(data).equals(checksums.get(chunkId))) {
            throw new IOException("checksum mismatch for " + chunkId + " on node " + id);
        }
        return data;
    }

    public boolean hasChunk(String chunkId) {
        return chunks.containsKey(chunkId);
    }

    /** Simulates disk-level corruption, for demo/test purposes. */
    public void corrupt(String chunkId) {
        byte[] data = chunks.get(chunkId);
        if (data != null && data.length > 0) {
            data[0] ^= 0xFF; // flip a byte; the stored checksum no longer matches
        }
    }

    public void heartbeat() {
        lastHeartbeat = System.currentTimeMillis();
    }

    public boolean isAlive(long timeoutMs) {
        return System.currentTimeMillis() - lastHeartbeat < timeoutMs;
    }

    @Override
    public String toString() {
        return id;
    }
}
