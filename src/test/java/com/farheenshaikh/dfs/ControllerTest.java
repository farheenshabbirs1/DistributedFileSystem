package com.farheenshaikh.dfs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Integration tests: every test here talks to a real cluster of {@link com.farheenshaikh.dfs.net.NodeServer}
 * instances over real TCP sockets via {@link TestCluster}. Node "failure" means actually
 * closing that node's server, not simulating a missed heartbeat -- the heartbeat/liveness
 * behavior under test is the real thing, not a stand-in for it.
 */
class ControllerTest {

    @Test
    void writeChunkReplicatesToExactlyReplicationFactorNodes() throws Exception {
        try (TestCluster cluster = new TestCluster(6, 3, 2000, 500, 200)) {
            cluster.controller.writeChunk("chunk-a", "payload".getBytes());

            assertEquals(3, cluster.controller.replicasOf("chunk-a").size());
        }
    }

    @Test
    void roundRobinAssignmentWrapsAcrossSuccessiveWrites() throws Exception {
        try (TestCluster cluster = new TestCluster(3, 1, 2000, 500, 200)) {
            Controller controller = cluster.controller;

            controller.writeChunk("chunk-a", "a".getBytes());
            controller.writeChunk("chunk-b", "b".getBytes());
            controller.writeChunk("chunk-c", "c".getBytes());
            controller.writeChunk("chunk-d", "d".getBytes());

            String first = controller.replicasOf("chunk-a").get(0);
            String second = controller.replicasOf("chunk-b").get(0);
            String third = controller.replicasOf("chunk-c").get(0);
            String fourth = controller.replicasOf("chunk-d").get(0);

            // With 3 nodes and 1 replica per write, the cursor wraps back to the first node on the 4th write.
            assertEquals(first, fourth);
            assertNotEquals(first, second);
            assertNotEquals(second, third);
        }
    }

    @Test
    void readChunkFallsBackToNextReplicaWhenOneIsCorruptedOverRpc() throws Exception {
        try (TestCluster cluster = new TestCluster(4, 2, 2000, 500, 200)) {
            Controller controller = cluster.controller;
            controller.writeChunk("chunk-a", "payload".getBytes());

            String firstReplicaId = controller.replicasOf("chunk-a").get(0);
            controller.corruptChunkOnNode(firstReplicaId, "chunk-a");

            assertArrayEquals("payload".getBytes(), controller.readChunk("chunk-a"));
        }
    }

    @Test
    void readChunkFailsWhenNoReplicaIsHealthy() throws Exception {
        try (TestCluster cluster = new TestCluster(2, 1, 2000, 500, 200)) {
            Controller controller = cluster.controller;
            controller.writeChunk("chunk-a", "payload".getBytes());

            String onlyReplica = controller.replicasOf("chunk-a").get(0);
            cluster.kill(onlyReplica);
            Thread.sleep(2200); // past the heartbeat timeout, with no other replica to fail over to

            org.junit.jupiter.api.Assertions.assertThrows(IOException.class,
                    () -> controller.readChunk("chunk-a"));
        }
    }

    /**
     * Mirrors a real compound failure: one replica's node is killed outright, and a second
     * replica's on-disk copy is corrupted (injected over its own Protobuf RPC). A single sweep
     * discovers the corruption (scrubbing that replica from the bookkeeping list) and adds one
     * new healthy replica to close the gap that was known about *before* the corruption was
     * discovered.
     */
    @Test
    void replicationSweepScrubsACorruptedReplicaAndHealsTheKnownDeficit() throws Exception {
        long heartbeatTimeoutMs = 350;
        long heartbeatIntervalMs = 100;
        try (TestCluster cluster = new TestCluster(6, 3, heartbeatTimeoutMs, 500, heartbeatIntervalMs)) {
            Controller controller = cluster.controller;

            controller.writeChunk("chunk-a", "payload".getBytes());
            List<String> initialReplicas = List.copyOf(controller.replicasOf("chunk-a"));
            assertEquals(3, initialReplicas.size());
            String corruptedId = initialReplicas.get(0);
            String killedId = initialReplicas.get(1);

            controller.corruptChunkOnNode(corruptedId, "chunk-a");
            cluster.kill(killedId);
            Thread.sleep(heartbeatTimeoutMs + 200); // let killedId age out of the heartbeat cache

            controller.replicationSweep();

            List<String> replicas = controller.replicasOf("chunk-a");
            assertFalse(replicas.contains(corruptedId), "the corrupted replica should be scrubbed from the list");
            assertEquals(2, cluster.aliveCleanReplicaCount("chunk-a"),
                    "first sweep only closes the deficit that was known before the corruption was found");
        }
    }

    /**
     * The sweep computes how many replicas to add *before* it discovers a source is corrupt, so
     * when the corrupted replica also had to be dropped, one sweep only recovers part of the gap.
     * A second sweep (the next poll cycle) finishes restoring the full replication factor -- the
     * system is eventually consistent across sweep cycles, not instantaneous within one. See the
     * README's "Known limitation" section for the full explanation.
     */
    @Test
    void fullyRestoringReplicationAfterCompoundFailureCanTakeTwoSweeps() throws Exception {
        long heartbeatTimeoutMs = 350;
        long heartbeatIntervalMs = 100;
        try (TestCluster cluster = new TestCluster(6, 3, heartbeatTimeoutMs, 500, heartbeatIntervalMs)) {
            Controller controller = cluster.controller;

            controller.writeChunk("chunk-a", "payload".getBytes());
            List<String> initialReplicas = List.copyOf(controller.replicasOf("chunk-a"));
            String corruptedId = initialReplicas.get(0);
            String killedId = initialReplicas.get(1);

            controller.corruptChunkOnNode(corruptedId, "chunk-a");
            cluster.kill(killedId);
            Thread.sleep(heartbeatTimeoutMs + 200);

            controller.replicationSweep();
            assertEquals(2, cluster.aliveCleanReplicaCount("chunk-a"), "after the first sweep");

            Thread.sleep(heartbeatIntervalMs * 2); // let another poll cycle's worth of time pass
            controller.replicationSweep();
            assertEquals(3, cluster.aliveCleanReplicaCount("chunk-a"), "after the second sweep");
        }
    }

    @Test
    void readChunkStaysCorrectThroughoutHealingRegardlessOfSweepTiming() throws Exception {
        long heartbeatTimeoutMs = 350;
        try (TestCluster cluster = new TestCluster(6, 3, heartbeatTimeoutMs, 500, 100)) {
            Controller controller = cluster.controller;

            controller.writeChunk("chunk-a", "payload".getBytes());
            List<String> initialReplicas = List.copyOf(controller.replicasOf("chunk-a"));
            controller.corruptChunkOnNode(initialReplicas.get(0), "chunk-a");
            cluster.kill(initialReplicas.get(1));
            Thread.sleep(heartbeatTimeoutMs + 200);

            // Even before any sweep runs, readChunk already races past the dead and corrupted replicas.
            assertArrayEquals("payload".getBytes(), controller.readChunk("chunk-a"));

            controller.replicationSweep();
            assertArrayEquals("payload".getBytes(), controller.readChunk("chunk-a"));
        }
    }
}
