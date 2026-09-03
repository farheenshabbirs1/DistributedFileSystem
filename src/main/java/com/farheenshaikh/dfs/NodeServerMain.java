package com.farheenshaikh.dfs;

import com.farheenshaikh.dfs.net.NodeServer;

import java.util.concurrent.CountDownLatch;

/**
 * Standalone entry point for running one storage node as its own OS process (and, in
 * {@code infra/}, its own container) instead of a thread inside {@link Demo}'s single JVM.
 *
 * <p>{@link NodeServer} itself was always a real, independently addressable TCP server -- this
 * class just gives it a process of its own to run in, configured from the environment instead of
 * being wired up in-process by a {@link Controller}. That's what turns "N storage nodes" into N
 * real, independently deployable/killable processes, the way {@code infra/docker-compose.yml}
 * runs them.
 *
 * <p>Configuration (environment variables):
 * <ul>
 *   <li>{@code NODE_ID} -- this node's id, e.g. {@code node-1}. Defaults to {@code node-1}.
 *   <li>{@code NODE_PORT} -- the TCP port to bind. Defaults to {@code 6000}.
 * </ul>
 *
 * <p>Run: {@code java -cp distributed-file-system.jar com.farheenshaikh.dfs.NodeServerMain}
 */
public final class NodeServerMain {

    private NodeServerMain() {
    }

    public static void main(String[] args) throws Exception {
        String nodeId = System.getenv().getOrDefault("NODE_ID", "node-1");
        int port = Integer.parseInt(System.getenv().getOrDefault("NODE_PORT", "6000"));

        NodeServer server = new NodeServer(nodeId, port);
        server.start();
        System.out.println("[" + nodeId + "] storage node listening on port " + server.port()
                + " (Protobuf-over-TCP)");

        // NodeServer's accept loop runs on a daemon thread, so this process needs something
        // non-daemon to keep it alive -- block forever, and clean up on SIGTERM/SIGINT
        // (docker stop / docker compose down send SIGTERM, which Java's shutdown hooks catch).
        CountDownLatch keepAlive = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[" + nodeId + "] shutting down");
            server.close();
            keepAlive.countDown();
        }));
        keepAlive.await();
    }
}
