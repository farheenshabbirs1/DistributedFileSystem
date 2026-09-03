package com.farheenshaikh.dfs;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the local storage engine only -- no networking involved. See
 * {@link ControllerTest} and {@link FileChunkerTest} for the networked, end-to-end behavior
 * (heartbeat-based liveness now lives in {@link HeartbeatMonitor}, driven over real RPCs).
 */
class StorageNodeTest {

    @Test
    void storesAndReadsBackIdenticalData() throws IOException {
        StorageNode node = new StorageNode("node-1");
        byte[] data = "chunk contents".getBytes();

        node.store("chunk-a", data);

        assertArrayEquals(data, node.read("chunk-a"));
    }

    @Test
    void readReturnsNullForMissingChunk() throws IOException {
        StorageNode node = new StorageNode("node-1");

        assertNull(node.read("does-not-exist"));
    }

    @Test
    void hasChunkReflectsWhatHasBeenStored() {
        StorageNode node = new StorageNode("node-1");
        assertFalse(node.hasChunk("chunk-a"));

        node.store("chunk-a", "data".getBytes());

        assertTrue(node.hasChunk("chunk-a"));
    }

    @Test
    void corruptedChunkFailsChecksumVerificationOnRead() {
        StorageNode node = new StorageNode("node-1");
        node.store("chunk-a", "chunk contents".getBytes());

        assertTrue(node.corrupt("chunk-a"));

        assertThrows(IOException.class, () -> node.read("chunk-a"));
    }

    @Test
    void corruptReturnsFalseForAMissingChunk() {
        StorageNode node = new StorageNode("node-1");

        assertFalse(node.corrupt("does-not-exist"));
    }

    @Test
    void storeClonesInputSoCallerMutationDoesNotAffectStoredData() throws IOException {
        StorageNode node = new StorageNode("node-1");
        byte[] data = "chunk contents".getBytes();

        node.store("chunk-a", data);
        data[0] = 0; // mutate the caller's array after storing

        assertNotEquals((byte) 0, node.read("chunk-a")[0]);
    }
}
