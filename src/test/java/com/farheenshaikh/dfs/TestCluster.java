package com.farheenshaikh.dfs;

import com.farheenshaikh.dfs.net.NodeServer;

import java.util.ArrayList;
import java.util.List;

/**
 * Test fixture: a real {@link Controller} wired up to a real cluster of {@link NodeServer}
 * instances, each bound to an OS-assigned free port on localhost. Every test that exercises
 * {@code Controller} talks to genuine TCP servers over genuine sockets -- there is no in-memory
 * shortcut being tested here, which is the point.
 */
final class TestCluster implements AutoCloseable {

    final Controller controller;
    private final List<NodeServer> servers = new ArrayList<>();

    /**
     * @param nodeCount           number of storage nodes to start
     * @param replicationFactor   passed straight to {@link Controller}
     * @param heartbeatTimeoutMs  passed straight to {@link Controller}
     * @param rpcTimeoutMs        passed straight to {@link Controller}
     * @param heartbeatIntervalMs how often the controller pings every node
     */
    TestCluster(int nodeCount, int replicationFactor, long heartbeatTimeoutMs, int rpcTimeoutMs,
                long heartbeatIntervalMs) throws Exception {
        controller = new Controller(replicationFactor, heartbeatTimeoutMs, rpcTimeoutMs);
        for (int i = 1; i <= nodeCount; i++) {
            NodeServer server = new NodeServer("node-" + i, /* port */ 0);
            server.start();
            servers.add(server);
            controller.registerNode(server);
        }
        controller.start(heartbeatIntervalMs);
        Thread.sleep(heartbeatIntervalMs + 150); // let the first round of heartbeats land
    }

    /** Kills one node's server outright, simulating a real node failure (not just a missed heartbeat). */
    void kill(String nodeId) {
        servers.stream().filter(s -> s.nodeId().equals(nodeId)).findFirst().ifPresent(NodeServer::close);
    }

    /** Counts replicas of a chunk that currently respond with a clean (checksum-valid) copy. */
    int aliveCleanReplicaCount(String chunkId) {
        int count = 0;
        for (String id : controller.replicasOf(chunkId)) {
            if (controller.isReplicaHealthy(id, chunkId)) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void close() {
        controller.close();
        servers.forEach(NodeServer::close);
    }
}
