package com.farheenshaikh.dfs;

import com.farheenshaikh.dfs.net.NodeServer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end demo: starts a real cluster of independently running {@link NodeServer} processes
 * (each its own thread-backed TCP server, communicating over Protocol Buffers), writes a
 * chunked, replicated file across them in parallel, kills one node outright and corrupts a
 * chunk's copy on another via RPC, then shows the controller's background heartbeat and
 * replication-sweep threads heal the cluster over the network so a subsequent parallel read
 * still recovers the original file.
 *
 * <p>Run: {@code mvn compile exec:java}
 */
public final class Demo {

    private static final int NODE_COUNT = 24; // real, independently addressable storage nodes
    private static final int REPLICATION_FACTOR = 3;
    private static final int CHUNK_SIZE = 64; // small on purpose, to exercise many chunks per file
    private static final long HEARTBEAT_INTERVAL_MS = 300;
    private static final long HEARTBEAT_TIMEOUT_MS = 1000;
    private static final int RPC_TIMEOUT_MS = 1000;

    private Demo() {
    }

    public static void main(String[] args) throws Exception {
        List<NodeServer> servers = new ArrayList<>();
        for (int i = 1; i <= NODE_COUNT; i++) {
            NodeServer server = new NodeServer("node-" + i, /* port */ 0); // 0 = OS-assigned free port
            server.start();
            servers.add(server);
        }
        System.out.println("Started " + servers.size() + " storage nodes, each its own TCP server "
                + "speaking Protobuf-over-socket (e.g. " + servers.get(0).nodeId() + " on port "
                + servers.get(0).port() + ")");

        Controller controller = new Controller(REPLICATION_FACTOR, HEARTBEAT_TIMEOUT_MS, RPC_TIMEOUT_MS);
        for (NodeServer server : servers) {
            controller.registerNode(server);
        }
        controller.start(HEARTBEAT_INTERVAL_MS);
        Thread.sleep(HEARTBEAT_INTERVAL_MS + 200); // let the first round of heartbeats land

        ScheduledExecutorService replication = Executors.newSingleThreadScheduledExecutor();
        replication.scheduleAtFixedRate(controller::replicationSweep, 500, 500, TimeUnit.MILLISECONDS);

        try {
            // 1. Write a file, chunked and replicated round-robin across the cluster, in parallel.
            byte[] original = "The quick brown fox jumps over the lazy dog. ".repeat(20).getBytes();
            long writeStart = System.currentTimeMillis();
            List<String> chunkIds = FileChunker.putFile(controller, "story.txt", original, CHUNK_SIZE);
            long writeMs = System.currentTimeMillis() - writeStart;
            System.out.println("\nWrote story.txt as " + chunkIds.size() + " chunk(s) in parallel ("
                    + writeMs + " ms), replication factor " + controller.replicationFactor());
            for (int i = 0; i < Math.min(3, chunkIds.size()); i++) {
                System.out.println("  " + chunkIds.get(i) + " -> " + controller.replicasOf(chunkIds.get(i)));
            }
            if (chunkIds.size() > 3) {
                System.out.println("  ... (" + (chunkIds.size() - 3) + " more chunks)");
            }

            // 2. Simulate a real node failure (shut its server down entirely) and on-disk corruption
            //    of another replica, discovered and injected over its own Protobuf RPC.
            String firstChunk = chunkIds.get(0);
            List<String> firstChunkReplicas = controller.replicasOf(firstChunk);
            String killedNodeId = firstChunkReplicas.get(0);
            String corruptedNodeId = firstChunkReplicas.get(1);

            System.out.println("\nKilling " + killedNodeId + " outright (its TCP server goes down)...");
            servers.stream().filter(s -> s.nodeId().equals(killedNodeId)).findFirst().get().close();

            System.out.println("Corrupting " + firstChunk + " on " + corruptedNodeId + " via RPC...");
            controller.corruptChunkOnNode(corruptedNodeId, firstChunk);

            // 3. Give the background heartbeat + replication-sweep threads time to detect and heal
            //    both failures. Restoring full replication after a compound failure like this can
            //    take more than one sweep cycle -- see the README for why.
            System.out.println("\nWaiting for heartbeat timeout + replication sweeps to heal the cluster...\n");
            Thread.sleep(HEARTBEAT_TIMEOUT_MS + 4 * 500 + 500);

            // 4. Read the file back in parallel -- should succeed via automatic failover / healed replicas.
            long readStart = System.currentTimeMillis();
            byte[] recovered = FileChunker.getFile(controller, chunkIds);
            long readMs = System.currentTimeMillis() - readStart;
            System.out.println("\nRead story.txt back in parallel (" + readMs + " ms)");
            System.out.println("Recovered file matches original: " + Arrays.equals(original, recovered));
        } finally {
            replication.shutdownNow();
            controller.close();
            servers.forEach(NodeServer::close);
        }
    }
}
