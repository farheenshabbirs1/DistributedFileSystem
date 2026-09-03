package com.farheenshaikh.dfs;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void corruptedChunkFailsChecksumVerificationOnRead() {
        StorageNode node = new StorageNode("node-1");
        node.store("chunk-a", "chunk contents".getBytes());

        node.corrupt("chunk-a");

        assertThrows(IOException.class, () -> node.read("chunk-a"));
    }

    @Test
    void isAliveReflectsHeartbeatRecency() throws InterruptedException {
        StorageNode node = new StorageNode("node-1");
        assertTrue(node.isAlive(1000));

        Thread.sleep(50);
        assertFalse(node.isAlive(10));

        node.heartbeat();
        assertTrue(node.isAlive(1000));
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
