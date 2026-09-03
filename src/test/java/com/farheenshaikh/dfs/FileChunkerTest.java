package com.farheenshaikh.dfs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Exercises FileChunker's parallel put/get against a real running cluster. */
class FileChunkerTest {

    @Test
    void roundTripsAFileThatFitsInOneChunk() throws Exception {
        try (TestCluster cluster = new TestCluster(4, 2, 2000, 500, 200)) {
            byte[] original = "short file".getBytes();

            List<String> chunkIds = FileChunker.putFile(cluster.controller, "small.txt", original, 64);
            byte[] recovered = FileChunker.getFile(cluster.controller, chunkIds);

            assertEquals(1, chunkIds.size());
            assertArrayEquals(original, recovered);
        }
    }

    @Test
    void roundTripsAFileSpanningManyChunksAcrossManyNodesInParallel() throws Exception {
        try (TestCluster cluster = new TestCluster(10, 2, 2000, 500, 200)) {
            byte[] original = "The quick brown fox jumps over the lazy dog. ".repeat(10).getBytes();

            long start = System.currentTimeMillis();
            List<String> chunkIds = FileChunker.putFile(cluster.controller, "story.txt", original, 20);
            byte[] recovered = FileChunker.getFile(cluster.controller, chunkIds);
            long elapsedMs = System.currentTimeMillis() - start;

            assertEquals((int) Math.ceil(original.length / 20.0), chunkIds.size());
            assertArrayEquals(original, recovered);
            // Not a strict timing assertion (this sandbox's scheduler can be noisy) -- just a
            // sanity check that ~20+ chunk RPCs aren't being run one at a time sequentially.
            assertEquals(true, elapsedMs < 5000, "put+get of " + chunkIds.size() + " chunks took " + elapsedMs + "ms");
        }
    }

    @Test
    void getFileFailsCleanlyWhenAChunkHasNoHealthyReplica() throws Exception {
        try (TestCluster cluster = new TestCluster(2, 1, 2000, 500, 200)) {
            byte[] original = "data".getBytes();
            List<String> chunkIds = FileChunker.putFile(cluster.controller, "f.txt", original, 64);

            String onlyReplica = cluster.controller.replicasOf(chunkIds.get(0)).get(0);
            cluster.kill(onlyReplica);
            Thread.sleep(2200); // past the heartbeat timeout, with no other replica to fail over to

            org.junit.jupiter.api.Assertions.assertThrows(IOException.class,
                    () -> FileChunker.getFile(cluster.controller, chunkIds));
        }
    }
}
