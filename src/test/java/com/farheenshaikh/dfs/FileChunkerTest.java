package com.farheenshaikh.dfs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FileChunkerTest {

    private Controller newCluster(int replicationFactor, int nodeCount) {
        Controller controller = new Controller(replicationFactor, 5000);
        for (int i = 1; i <= nodeCount; i++) {
            controller.registerNode(new StorageNode("node-" + i));
        }
        return controller;
    }

    @Test
    void roundTripsAFileThatFitsInOneChunk() throws IOException {
        Controller controller = newCluster(2, 4);
        byte[] original = "short file".getBytes();

        List<String> chunkIds = FileChunker.putFile(controller, "small.txt", original, 64);
        byte[] recovered = FileChunker.getFile(controller, chunkIds);

        assertEquals(1, chunkIds.size());
        assertArrayEquals(original, recovered);
    }

    @Test
    void roundTripsAFileSpanningMultipleChunks() throws IOException {
        Controller controller = newCluster(2, 4);
        byte[] original = "The quick brown fox jumps over the lazy dog. ".repeat(5).getBytes();

        List<String> chunkIds = FileChunker.putFile(controller, "story.txt", original, 20);
        byte[] recovered = FileChunker.getFile(controller, chunkIds);

        assertEquals((int) Math.ceil(original.length / 20.0), chunkIds.size());
        assertArrayEquals(original, recovered);
    }
}
