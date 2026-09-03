package com.farheenshaikh.dfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end demo: writes a chunked, replicated file across a 6-node cluster,
 * simulates a node failure and on-disk corruption, then shows the background
 * heartbeat + replication-sweep threads heal the cluster so a subsequent read
 * still recovers the original file.
 *
 * <p>Run: {@code mvn compile exec:java}
 */
public final class Demo {

    private static final int CHUNK_SIZE = 64; // small on purpose, to exercise multi-chunk files

    private Demo() {
    }

    public static void main(String[] args) throws Exception {
        Controller controller = new Controller(/* replicationFactor */ 3, /* heartbeatTimeoutMs */ 2000);

        List<StorageNode> allNodes = new ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            StorageNode node = new StorageNode("node-" + i);
            controller.registerNode(node);
            allNodes.add(node);
        }

        // Heartbeat sender: every node "checks in" every 500ms.
        ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor();
        heartbeats.scheduleAtFixedRate(() -> allNodes.forEach(StorageNode::heartbeat), 0, 500, TimeUnit.MILLISECONDS);

        // Replication monitor: poll-driven re-replication sweep every 1s.
        ScheduledExecutorService replication = Executors.newSingleThreadScheduledExecutor();
        replication.scheduleAtFixedRate(controller::replicationSweep, 1, 1, TimeUnit.SECONDS);

        try {
            // 1. Write a file, chunked and replicated round-robin across the 6 nodes.
            byte[] original = "The quick brown fox jumps over the lazy dog. ".repeat(10).getBytes();
            List<String> chunkIds = FileChunker.putFile(controller, "story.txt", original, CHUNK_SIZE);
            System.out.println("Wrote story.txt as " + chunkIds.size() + " chunk(s), replication factor "
                    + controller.replicationFactor());
            chunkIds.forEach(id -> System.out.println("  " + id + " -> " + controller.replicasOf(id)));

            // 2. Simulate a node failure (stop its heartbeats) and on-disk corruption on another.
            Thread.sleep(300);
            System.out.println("\nSimulating node-2 failure (heartbeats stop)...");
            allNodes.removeIf(n -> n.id.equals("node-2")); // no longer heartbeat-ed, so it goes stale and "dies"

            System.out.println("Simulating on-disk corruption of " + chunkIds.get(0) + " on node-1...");
            findNode(allNodes, "node-1").corrupt(chunkIds.get(0));

            // 3. Give the background threads time to detect and heal both failures.
            System.out.println("\nWaiting for heartbeat timeout + replication sweep to heal the cluster...\n");
            Thread.sleep(3500);

            // 4. Read the file back -- should succeed via automatic failover / healed replicas.
            byte[] recovered = FileChunker.getFile(controller, chunkIds);
            System.out.println("\nRecovered file matches original: " + Arrays.equals(original, recovered));
        } finally {
            heartbeats.shutdownNow();
            replication.shutdownNow();
        }
    }

    private static StorageNode findNode(List<StorageNode> nodes, String id) {
        return nodes.stream()
                .filter(n -> n.id.equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("node not found: " + id));
    }
}
