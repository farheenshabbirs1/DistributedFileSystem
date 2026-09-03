package com.farheenshaikh.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Cluster coordinator: tracks node membership and liveness, assigns chunk
 * placement via round-robin partitioning, and heals replication gaps.
 *
 * <p>This class holds only metadata and coordination logic -- it never touches
 * chunk bytes directly, only through {@link StorageNode}. Background polling
 * (heartbeats, the replication sweep) is driven from the outside (see
 * {@link Demo}); {@link #replicationSweep()} itself is a plain synchronous
 * method, which is what makes it straightforward to unit test.
 */
public class Controller {

    private final Map<String, StorageNode> nodes = new ConcurrentHashMap<>();
    private final Map<String, List<String>> chunkReplicas = new ConcurrentHashMap<>();
    private final int replicationFactor;
    private final long heartbeatTimeoutMs;
    private int roundRobinCursor = 0;

    public Controller(int replicationFactor, long heartbeatTimeoutMs) {
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("replicationFactor must be >= 1");
        }
        this.replicationFactor = replicationFactor;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public void registerNode(StorageNode node) {
        nodes.put(node.id, node);
    }

    public int replicationFactor() {
        return replicationFactor;
    }

    /** The node ids currently believed to hold a replica of this chunk (may include stale entries). */
    public List<String> replicasOf(String chunkId) {
        return chunkReplicas.getOrDefault(chunkId, List.of());
    }

    /** Heartbeat-based failure detection: alive = heartbeat received within the timeout window. */
    public synchronized List<StorageNode> aliveNodes() {
        List<StorageNode> alive = new ArrayList<>();
        for (StorageNode n : nodes.values()) {
            if (n.isAlive(heartbeatTimeoutMs)) {
                alive.add(n);
            }
        }
        alive.sort(Comparator.comparing(n -> n.id));
        return alive;
    }

    /** Round-robin partitioning: spreads chunk placement evenly across the alive-node ring. */
    synchronized List<StorageNode> assign(int count) {
        List<StorageNode> ring = aliveNodes();
        List<StorageNode> chosen = new ArrayList<>();
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

    public void writeChunk(String chunkId, byte[] data) {
        List<StorageNode> targets = assign(replicationFactor);
        if (targets.isEmpty()) {
            throw new IllegalStateException("no alive storage nodes available to write " + chunkId);
        }
        List<String> replicaIds = new CopyOnWriteArrayList<>();
        for (StorageNode n : targets) {
            n.store(chunkId, data);
            replicaIds.add(n.id);
        }
        chunkReplicas.put(chunkId, replicaIds);
    }

    /** Poll-driven re-replication: called on a timer, heals any chunk below its replication factor. */
    public void replicationSweep() {
        for (Map.Entry<String, List<String>> entry : chunkReplicas.entrySet()) {
            healChunk(entry.getKey(), entry.getValue());
        }
    }

    private void healChunk(String chunkId, List<String> replicaIds) {
        List<StorageNode> aliveReplicas = new ArrayList<>();
        for (String id : replicaIds) {
            StorageNode n = nodes.get(id);
            if (n != null && n.isAlive(heartbeatTimeoutMs)) {
                aliveReplicas.add(n);
            }
        }
        if (aliveReplicas.size() >= replicationFactor) {
            return;
        }

        // Find a healthy, non-corrupt source to copy from -- this also scrubs any replica that
        // turns out to be corrupt (its id is dropped from replicaIds as a side effect).
        StorageNode source = null;
        byte[] data = null;
        for (StorageNode candidate : aliveReplicas) {
            try {
                data = candidate.read(chunkId);
                source = candidate;
                break;
            } catch (IOException corrupt) {
                System.out.println("[scrub] " + corrupt.getMessage() + " -- dropping replica");
                replicaIds.remove(candidate.id);
            }
        }
        if (source == null) {
            return; // no healthy replica available yet -- try again on the next sweep
        }

        Set<String> haveIt = new HashSet<>(replicaIds);
        int deficit = replicationFactor - aliveReplicas.size();
        List<StorageNode> candidates = assign(deficit + haveIt.size()); // over-fetch, then filter
        int healed = 0;
        for (StorageNode target : candidates) {
            if (healed >= deficit) {
                break;
            }
            if (haveIt.contains(target.id)) {
                continue;
            }
            target.store(chunkId, data);
            replicaIds.add(target.id);
            healed++;
            System.out.println("[replication] healed " + chunkId + " -> " + target.id
                    + " (now " + replicaIds.size() + "/" + replicationFactor + " replicas)");
        }
    }

    /** Reads a chunk, transparently trying every alive replica until one passes checksum verification. */
    public byte[] readChunk(String chunkId) throws IOException {
        for (String id : replicasOf(chunkId)) {
            StorageNode n = nodes.get(id);
            if (n == null || !n.isAlive(heartbeatTimeoutMs)) {
                continue;
            }
            try {
                return n.read(chunkId);
            } catch (IOException corrupt) {
                System.out.println("[read] " + corrupt.getMessage() + " -- trying next replica");
            }
        }
        throw new IOException("no healthy replica available for chunk " + chunkId);
    }
}
