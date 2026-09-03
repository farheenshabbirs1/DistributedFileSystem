package com.farheenshaikh.dfs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControllerTest {

    /** A controller plus direct references to the nodes it was seeded with, for test control. */
    private static final class Cluster {
        final Controller controller;
        final List<StorageNode> nodes;

        Cluster(Controller controller, List<StorageNode> nodes) {
            this.controller = controller;
            this.nodes = nodes;
        }

        StorageNode nodeById(String id) {
            return nodes.stream().filter(n -> n.id.equals(id)).findFirst()
                    .orElseThrow(() -> new IllegalStateException("no such node: " + id));
        }

        /** Heartbeats every node except the given ids -- used to let specific nodes go stale. */
        void heartbeatAllExcept(String... excludedIds) {
            List<String> excluded = List.of(excludedIds);
            for (StorageNode n : nodes) {
                if (!excluded.contains(n.id)) {
                    n.heartbeat();
                }
            }
        }

        /** Counts replicas of a chunk that are both alive (heartbeat-wise) and pass checksum verification. */
        int aliveCleanReplicaCount(String chunkId, long heartbeatTimeoutMs) throws IOException {
            int count = 0;
            for (String id : controller.replicasOf(chunkId)) {
                StorageNode n = nodeById(id);
                if (n.isAlive(heartbeatTimeoutMs) && n.read(chunkId) != null) {
                    count++;
                }
            }
            return count;
        }
    }

    private Cluster newCluster(int replicationFactor, long heartbeatTimeoutMs, int nodeCount) {
        Controller controller = new Controller(replicationFactor, heartbeatTimeoutMs);
        List<StorageNode> nodes = new ArrayList<>();
        for (int i = 1; i <= nodeCount; i++) {
            StorageNode node = new StorageNode("node-" + i);
            controller.registerNode(node);
            nodes.add(node);
        }
        return new Cluster(controller, nodes);
    }

    @Test
    void writeChunkReplicatesToExactlyReplicationFactorNodes() {
        Cluster cluster = newCluster(3, 5000, 6);

        cluster.controller.writeChunk("chunk-a", "payload".getBytes());

        assertEquals(3, cluster.controller.replicasOf("chunk-a").size());
    }

    @Test
    void roundRobinAssignmentWrapsAcrossSuccessiveWrites() {
        Cluster cluster = newCluster(1, 5000, 3);
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

    @Test
    void readChunkFallsBackToNextReplicaWhenOneIsCorrupted() throws IOException {
        Cluster cluster = newCluster(2, 5000, 4);
        cluster.controller.writeChunk("chunk-a", "payload".getBytes());

        String firstReplicaId = cluster.controller.replicasOf("chunk-a").get(0);
        cluster.nodeById(firstReplicaId).corrupt("chunk-a");

        assertArrayEquals("payload".getBytes(), cluster.controller.readChunk("chunk-a"));
    }

    /**
     * Mirrors the demo scenario: one replica goes stale (its node stops heartbeating) while another
     * replica's on-disk copy is corrupted. A single sweep discovers the corruption (scrubbing that
     * replica from the bookkeeping list) and adds one new healthy replica to close the gap that was
     * known about *before* the corruption was discovered.
     */
    @Test
    void replicationSweepScrubsACorruptedReplicaAndHealsTheKnownDeficit() throws Exception {
        long heartbeatTimeoutMs = 100;
        Cluster cluster = newCluster(3, heartbeatTimeoutMs, 6);
        Controller controller = cluster.controller;

        controller.writeChunk("chunk-a", "payload".getBytes());
        List<String> initialReplicas = List.copyOf(controller.replicasOf("chunk-a"));
        assertEquals(3, initialReplicas.size());
        String corruptedId = initialReplicas.get(0);
        String staleId = initialReplicas.get(1);

        cluster.nodeById(corruptedId).corrupt("chunk-a");
        letNodeGoStale(cluster, heartbeatTimeoutMs, staleId);

        controller.replicationSweep();

        List<String> replicas = controller.replicasOf("chunk-a");
        assertFalse(replicas.contains(corruptedId), "the corrupted replica should be scrubbed from the list");
        assertTrue(cluster.aliveCleanReplicaCount("chunk-a", heartbeatTimeoutMs) >= 2,
                "the sweep should add at least one healthy replica toward the target");
    }

    /**
     * The sweep computes how many replicas to add *before* it discovers a source is corrupt, so
     * when the corrupted replica also had to be dropped, one sweep only recovers part of the gap.
     * A second sweep (the next poll cycle) finishes restoring the full replication factor -- the
     * system is eventually consistent across sweep cycles, not instantaneous within one.
     */
    @Test
    void fullyRestoringReplicationAfterCorruptionAndStalenessCanTakeTwoSweeps() throws Exception {
        long heartbeatTimeoutMs = 100;
        Cluster cluster = newCluster(3, heartbeatTimeoutMs, 6);
        Controller controller = cluster.controller;

        controller.writeChunk("chunk-a", "payload".getBytes());
        List<String> initialReplicas = List.copyOf(controller.replicasOf("chunk-a"));
        String corruptedId = initialReplicas.get(0);
        String staleId = initialReplicas.get(1);

        cluster.nodeById(corruptedId).corrupt("chunk-a");
        letNodeGoStale(cluster, heartbeatTimeoutMs, staleId);

        controller.replicationSweep();
        assertEquals(2, cluster.aliveCleanReplicaCount("chunk-a", heartbeatTimeoutMs),
                "first sweep only closes the deficit that was known before the corruption was found");

        cluster.heartbeatAllExcept(staleId); // simulate time passing between poll cycles
        controller.replicationSweep();
        assertEquals(3, cluster.aliveCleanReplicaCount("chunk-a", heartbeatTimeoutMs),
                "second sweep restores the full replication factor");
    }

    @Test
    void readChunkStaysCorrectThroughoutHealingRegardlessOfSweepTiming() throws Exception {
        long heartbeatTimeoutMs = 100;
        Cluster cluster = newCluster(3, heartbeatTimeoutMs, 6);
        Controller controller = cluster.controller;

        controller.writeChunk("chunk-a", "payload".getBytes());
        List<String> initialReplicas = List.copyOf(controller.replicasOf("chunk-a"));
        cluster.nodeById(initialReplicas.get(0)).corrupt("chunk-a");
        letNodeGoStale(cluster, heartbeatTimeoutMs, initialReplicas.get(1));

        // Even before any sweep runs, readChunk already skips the dead and corrupted replicas.
        assertArrayEquals("payload".getBytes(), controller.readChunk("chunk-a"));

        controller.replicationSweep();
        assertArrayEquals("payload".getBytes(), controller.readChunk("chunk-a"));
    }

    private void letNodeGoStale(Cluster cluster, long heartbeatTimeoutMs, String staleId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + (heartbeatTimeoutMs * 3);
        while (System.currentTimeMillis() < deadline) {
            cluster.heartbeatAllExcept(staleId);
            Thread.sleep(Math.max(1, heartbeatTimeoutMs / 5));
        }
    }
}
