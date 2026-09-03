package com.farheenshaikh.dfs;

import com.farheenshaikh.dfs.net.NodeEndpoint;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Standalone entry point for running the cluster {@link Controller} as its own process, talking
 * to storage nodes over the network (real host:port addresses, e.g. other containers) instead of
 * nodes registered in-process the way {@link Demo} does. This is what makes
 * {@code infra/docker-compose.yml}'s cluster a genuine multi-process deployment: the controller
 * container and each storage-node container are independent JVMs that only ever talk to each
 * other over TCP + Protobuf, exactly like {@link com.farheenshaikh.dfs.net.NodeClient} /
 * {@link com.farheenshaikh.dfs.net.NodeServer} were built to do.
 *
 * <p>On startup, once the node cluster has had time to answer its first heartbeat round, this
 * runs the same write/read smoke test {@link Demo} runs locally -- write a small file across the
 * real cluster, read it back, confirm it round-trips -- so a fresh {@code docker compose up} logs
 * proof the containers are actually talking to each other, not just that they started. After
 * that it keeps running as a real long-lived coordinator process, logging cluster health on an
 * interval.
 *
 * <p>Configuration (environment variables):
 * <ul>
 *   <li>{@code NODE_ENDPOINTS} -- required. Comma-separated {@code id=host:port} pairs, e.g.
 *       {@code node-1=storage-node-1:6000,node-2=storage-node-2:6000}.
 *   <li>{@code REPLICATION_FACTOR} -- default {@code 3}.
 *   <li>{@code HEARTBEAT_INTERVAL_MS} -- default {@code 1000}.
 *   <li>{@code HEARTBEAT_TIMEOUT_MS} -- default {@code 3000}.
 *   <li>{@code RPC_TIMEOUT_MS} -- default {@code 2000}.
 *   <li>{@code REPLICATION_SWEEP_INTERVAL_MS} -- default {@code 2000}.
 *   <li>{@code STATUS_LOG_INTERVAL_MS} -- default {@code 10000}.
 * </ul>
 *
 * <p>Run: {@code java -cp distributed-file-system.jar com.farheenshaikh.dfs.ControllerMain}
 */
public final class ControllerMain {

    private ControllerMain() {
    }

    public static void main(String[] args) throws Exception {
        String endpointsSpec = System.getenv("NODE_ENDPOINTS");
        if (endpointsSpec == null || endpointsSpec.isBlank()) {
            System.err.println("NODE_ENDPOINTS is required, e.g. "
                    + "node-1=storage-node-1:6000,node-2=storage-node-2:6000");
            System.exit(1);
        }

        int replicationFactor = envInt("REPLICATION_FACTOR", 3);
        long heartbeatIntervalMs = envLong("HEARTBEAT_INTERVAL_MS", 1000);
        long heartbeatTimeoutMs = envLong("HEARTBEAT_TIMEOUT_MS", 3000);
        int rpcTimeoutMs = envInt("RPC_TIMEOUT_MS", 2000);
        long sweepIntervalMs = envLong("REPLICATION_SWEEP_INTERVAL_MS", 2000);
        long statusLogIntervalMs = envLong("STATUS_LOG_INTERVAL_MS", 10000);

        List<NodeEndpoint> endpoints = parseEndpoints(endpointsSpec);

        Controller controller = new Controller(replicationFactor, heartbeatTimeoutMs, rpcTimeoutMs);
        for (NodeEndpoint endpoint : endpoints) {
            controller.registerNode(endpoint);
        }
        System.out.println("[controller] registered " + endpoints.size() + " node(s): " + endpoints);

        controller.start(heartbeatIntervalMs);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(controller::replicationSweep, sweepIntervalMs, sweepIntervalMs,
                TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(() -> logStatus(controller), statusLogIntervalMs, statusLogIntervalMs,
                TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[controller] shutting down");
            scheduler.shutdownNow();
            controller.close();
        }));

        // Give the first couple of heartbeat rounds time to land before trying to write anything --
        // otherwise assign() would see zero alive nodes on a cold start.
        Thread.sleep(Math.max(heartbeatIntervalMs * 3, 1000));
        runSmokeTest(controller);

        new CountDownLatch(1).await(); // run forever; the shutdown hook above handles cleanup
    }

    private static void runSmokeTest(Controller controller) {
        try {
            byte[] original = ("Hello from the containerized DFS cluster. ").repeat(20)
                    .getBytes(StandardCharsets.UTF_8);
            List<String> chunkIds = FileChunker.putFile(controller, "smoke-test.txt", original, 64);
            byte[] recovered = FileChunker.getFile(controller, chunkIds);
            boolean ok = Arrays.equals(original, recovered);
            System.out.println("[controller] smoke test: wrote " + chunkIds.size()
                    + " chunk(s) across the real cluster, read them back, matches original: " + ok);
        } catch (Exception e) {
            System.out.println("[controller] smoke test failed (nodes may still be starting up): "
                    + e.getMessage());
        }
    }

    private static void logStatus(Controller controller) {
        List<NodeEndpoint> alive = controller.aliveNodes();
        System.out.println("[controller] status: " + alive.size() + " node(s) alive: " + alive);
    }

    private static List<NodeEndpoint> parseEndpoints(String spec) {
        return Arrays.stream(spec.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(ControllerMain::parseEndpoint)
                .toList();
    }

    private static NodeEndpoint parseEndpoint(String entry) {
        String[] idAndAddress = entry.split("=", 2);
        if (idAndAddress.length != 2) {
            throw new IllegalArgumentException("malformed NODE_ENDPOINTS entry (expected id=host:port): " + entry);
        }
        String id = idAndAddress[0];
        String[] hostAndPort = idAndAddress[1].split(":", 2);
        if (hostAndPort.length != 2) {
            throw new IllegalArgumentException("malformed NODE_ENDPOINTS entry (expected id=host:port): " + entry);
        }
        return new NodeEndpoint(id, hostAndPort[0], Integer.parseInt(hostAndPort[1]));
    }

    private static int envInt(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value.trim());
    }

    private static long envLong(String name, long defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Long.parseLong(value.trim());
    }
}
