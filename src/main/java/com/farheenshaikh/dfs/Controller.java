package com.farheenshaikh.dfs;

import com.farheenshaikh.dfs.net.NodeClient;
import com.farheenshaikh.dfs.net.NodeEndpoint;
import com.farheenshaikh.dfs.net.NodeServer;
import com.farheenshaikh.dfs.proto.DfsProto.ReadChunkResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Cluster coordinator: tracks node membership and liveness over the network, assigns chunk
 * placement via round-robin partitioning, and heals replication gaps -- all by issuing real
 * Protobuf RPCs ({@link NodeClient}) to independently running {@link NodeServer} processes,
 * never by touching chunk bytes directly.
 *
 * <p>Chunk writes fan out to all assigned replicas in parallel, and reads race all currently
 * alive replicas in parallel and return the first one that passes checksum verification --
 * this is the "parallel file storage and retrieval" the node count is meant to exercise: with
 * N replicas or M chunks in flight, wall-clock cost is roughly one round trip, not N or M.
 */
public class Controller implements AutoCloseable {

    private final Map<String, NodeEndpoint> endpoints = new ConcurrentHashMap<>();
    private final Map<String, NodeClient> clients = new ConcurrentHashMap<>();
    private final Map<String, List<String>> chunkReplicas = new ConcurrentHashMap<>();
    private final int replicationFactor;
    private final int rpcTimeoutMs;
    private final HeartbeatMonitor heartbeatMonitor;
    private final ExecutorService ioPool;
    private int roundRobinCursor = 0;

    public Controller(int replicationFactor, long heartbeatTimeoutMs, int rpcTimeoutMs) {
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("replicationFactor must be >= 1");
        }
        this.replicationFactor = replicationFactor;
        this.rpcTimeoutMs = rpcTimeoutMs;
        this.heartbeatMonitor = new HeartbeatMonitor(clients, heartbeatTimeoutMs);
        this.ioPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "dfs-io");
            t.setDaemon(true);
            return t;
        });
    }

    public void registerNode(NodeEndpoint endpoint) {
        endpoints.put(endpoint.id, endpoint);
        clients.put(endpoint.id, new NodeClient(endpoint, rpcTimeoutMs));
    }

    /** Convenience overload: registers a locally running NodeServer at its own id/port. */
    public void registerNode(NodeServer server) {
        registerNode(new NodeEndpoint(server.nodeId(), "localhost", server.port()));
    }

    /** Starts the background heartbeat poller (pings every node every {@code intervalMs}). */
    public void start(long intervalMs) {
        heartbeatMonitor.start(intervalMs);
    }

    public int replicationFactor() {
        return replicationFactor;
    }

    /** The node ids currently believed to hold a replica of this chunk (may include stale entries). */
    public List<String> replicasOf(String chunkId) {
        return chunkReplicas.getOrDefault(chunkId, List.of());
    }

    /** Heartbeat-based failure detection: alive = a heartbeat RPC answered within the timeout window. */
    public List<NodeEndpoint> aliveNodes() {
        List<NodeEndpoint> alive = new ArrayList<>();
        for (NodeEndpoint endpoint : endpoints.values()) {
            if (heartbeatMonitor.isAlive(endpoint.id)) {
                alive.add(endpoint);
            }
        }
        alive.sort(Comparator.comparing(e -> e.id));
        return alive;
    }

    /** Round-robin partitioning: spreads chunk placement evenly across the alive-node ring. */
    synchronized List<NodeEndpoint> assign(int count) {
        List<NodeEndpoint> ring = aliveNodes();
        List<NodeEndpoint> chosen = new ArrayList<>();
        if (ring.isEmpty()) {
            return chosen;
        }
        int start = Math.floorMod(roundRobinCursor++, ring.size());
        int take = Math.min(count, ring.size());
        for (int i = 0; i < take; i++) {
            chosen.add(ring.get((start + i) % ring.size()));
        }
        return chosen;
    }

    /** Writes a chunk to {@code replicationFactor} nodes, in parallel. */
    public void writeChunk(String chunkId, byte[] data) throws IOException {
        List<NodeEndpoint> targets = assign(replicationFactor);
        if (targets.isEmpty()) {
            throw new IOException("no alive storage nodes available to write " + chunkId);
        }

        List<Future<String>> futures = new ArrayList<>();
        for (NodeEndpoint target : targets) {
            futures.add(ioPool.submit(() -> {
                clients.get(target.id).storeChunk(chunkId, data);
                return target.id;
            }));
        }

        List<String> replicaIds = new CopyOnWriteArrayList<>();
        for (Future<String> future : futures) {
            try {
                replicaIds.add(future.get());
            } catch (ExecutionException | InterruptedException failedWrite) {
                System.out.println("[write] failed to store " + chunkId + " on a target: "
                        + failedWrite.getMessage());
            }
        }
        if (replicaIds.isEmpty()) {
            throw new IOException("failed to store " + chunkId + " on any node");
        }
        chunkReplicas.put(chunkId, replicaIds);
    }

    /** Test/demo hook: asks a specific node to simulate on-disk corruption of one of its chunks. */
    public boolean corruptChunkOnNode(String nodeId, String chunkId) throws IOException {
        NodeClient client = clients.get(nodeId);
        if (client == null) {
            throw new IOException("unknown node " + nodeId);
        }
        return client.corruptChunk(chunkId);
    }

    /** Poll-driven re-replication: called on a timer, heals any chunk below its replication factor. */
    public void replicationSweep() {
        for (Map.Entry<String, List<String>> entry : chunkReplicas.entrySet()) {
            healChunk(entry.getKey(), entry.getValue());
        }
    }

    private void healChunk(String chunkId, List<String> replicaIds) {
        List<String> aliveReplicaIds = new ArrayList<>();
        for (String id : replicaIds) {
            if (heartbeatMonitor.isAlive(id)) {
                aliveReplicaIds.add(id);
            }
        }
        if (aliveReplicaIds.size() >= replicationFactor) {
            return;
        }

        String sourceId = null;
        byte[] data = null;
        for (String candidateId : aliveReplicaIds) {
            NodeClient client = clients.get(candidateId);
            try {
                ReadChunkResponse response = client.readChunk(chunkId);
                if (response.getStatus() == ReadChunkResponse.Status.OK) {
                    sourceId = candidateId;
                    data = response.getData().toByteArray();
                    break;
                } else if (response.getStatus() == ReadChunkResponse.Status.CORRUPTED) {
                    System.out.println("[scrub] checksum mismatch for " + chunkId + " on node "
                            + candidateId + " -- dropping replica");
                    replicaIds.remove(candidateId);
                }
            } catch (IOException unreachable) {
                // became unreachable between the heartbeat check and now; try the next candidate
            }
        }
        if (sourceId == null) {
            return; // no healthy replica available yet -- try again on the next sweep
        }

        Set<String> haveIt = new HashSet<>(replicaIds);
        int deficit = replicationFactor - aliveReplicaIds.size();
        List<NodeEndpoint> candidates = assign(deficit + haveIt.size()); // over-fetch, then filter
        byte[] payload = data;
        int healed = 0;
        for (NodeEndpoint target : candidates) {
            if (healed >= deficit) {
                break;
            }
            if (haveIt.contains(target.id)) {
                continue;
            }
            try {
                clients.get(target.id).storeChunk(chunkId, payload);
                replicaIds.add(target.id);
                healed++;
                System.out.println("[replication] healed " + chunkId + " -> " + target.id
                        + " (now " + replicaIds.size() + "/" + replicationFactor + " replicas)");
            } catch (IOException failedWrite) {
                System.out.println("[replication] failed to heal " + chunkId + " onto " + target.id
                        + ": " + failedWrite.getMessage());
            }
        }
    }

    /**
     * Reads a chunk by racing RPCs to every alive replica in parallel and returning the first
     * one that passes checksum verification -- a single slow or dead replica doesn't add latency
     * as long as another replica answers.
     */
    public byte[] readChunk(String chunkId) throws IOException {
        List<String> candidateIds = new ArrayList<>();
        for (String id : replicasOf(chunkId)) {
            if (heartbeatMonitor.isAlive(id)) {
                candidateIds.add(id);
            }
        }
        if (candidateIds.isEmpty()) {
            throw new IOException("no healthy replica available for chunk " + chunkId);
        }

        CompletionService<ReadOutcome> completion = new ExecutorCompletionService<>(ioPool);
        for (String id : candidateIds) {
            NodeClient client = clients.get(id);
            completion.submit(() -> attemptRead(client, id, chunkId));
        }

        for (int i = 0; i < candidateIds.size(); i++) {
            try {
                ReadOutcome outcome = completion.take().get();
                if (outcome.success) {
                    return outcome.data;
                }
                System.out.println("[read] " + outcome.message + " -- trying next replica");
            } catch (ExecutionException | InterruptedException unexpected) {
                // treat as a failed attempt and keep waiting for the remaining candidates
            }
        }
        throw new IOException("no healthy replica available for chunk " + chunkId);
    }

    private ReadOutcome attemptRead(NodeClient client, String nodeId, String chunkId) {
        try {
            ReadChunkResponse response = client.readChunk(chunkId);
            if (response.getStatus() == ReadChunkResponse.Status.OK) {
                return ReadOutcome.ok(response.getData().toByteArray());
            }
            String reason = response.getStatus() == ReadChunkResponse.Status.CORRUPTED
                    ? "checksum mismatch for " + chunkId + " on node " + nodeId
                    : chunkId + " not found on node " + nodeId;
            return ReadOutcome.failure(reason);
        } catch (IOException unreachable) {
            return ReadOutcome.failure("node " + nodeId + " unreachable: " + unreachable.getMessage());
        }
    }

    @Override
    public void close() {
        heartbeatMonitor.close();
        ioPool.shutdownNow();
    }

    /**
     * Visible for testing: reads a chunk from one specific node directly (bypassing the
     * alive-node race in {@link #readChunk}), so tests can assert exactly which replicas are
     * currently healthy without depending on read failover to mask the answer.
     */
    boolean isReplicaHealthy(String nodeId, String chunkId) {
        NodeClient client = clients.get(nodeId);
        if (client == null) {
            return false;
        }
        try {
            return client.readChunk(chunkId).getStatus() == ReadChunkResponse.Status.OK;
        } catch (IOException unreachable) {
            return false;
        }
    }

    private static final class ReadOutcome {
        final boolean success;
        final byte[] data;
        final String message;

        private ReadOutcome(boolean success, byte[] data, String message) {
            this.success = success;
            this.data = data;
            this.message = message;
        }

        static ReadOutcome ok(byte[] data) {
            return new ReadOutcome(true, data, null);
        }

        static ReadOutcome failure(String message) {
            return new ReadOutcome(false, null, message);
        }
    }
}
