package com.farheenshaikh.dfs;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The local storage engine embedded inside a {@link com.farheenshaikh.dfs.net.NodeServer}:
 * holds chunk bytes plus a SHA-256 checksum for each chunk. This class itself does no
 * networking -- it's the thing a NodeServer calls into after decoding an RPC.
 */
public class StorageNode {

    public final String id;
    private final Map<String, byte[]> chunks = new ConcurrentHashMap<>();
    private final Map<String, String> checksums = new ConcurrentHashMap<>();

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

    /** Simulates disk-level corruption, for demo/test purposes. Returns false if the chunk is absent. */
    public boolean corrupt(String chunkId) {
        byte[] data = chunks.get(chunkId);
        if (data == null || data.length == 0) {
            return false;
        }
        data[0] ^= 0xFF; // flip a byte; the stored checksum no longer matches
        return true;
    }

    @Override
    public String toString() {
        return id;
    }
}
