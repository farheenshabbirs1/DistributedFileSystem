package com.farheenshaikh.dfs;

import com.farheenshaikh.dfs.net.NodeClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Heartbeat-based failure detection over the network: on a fixed interval, pings every
 * registered node's real {@code Heartbeat} RPC (in parallel, so N nodes cost one round-trip's
 * worth of wall-clock time, not N) and records the timestamp of each successful reply. A node
 * counts as alive if it answered within the last {@code heartbeatTimeoutMs}; a node that is
 * down, unreachable, or too slow simply never refreshes its timestamp and ages out.
 */
public class HeartbeatMonitor implements AutoCloseable {

    private final Map<String, NodeClient> clients;
    private final Map<String, Long> lastSeenAt = new ConcurrentHashMap<>();
    private final long heartbeatTimeoutMs;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService pingPool;
    private ScheduledFuture<?> pollingTask;

    public HeartbeatMonitor(Map<String, NodeClient> clients, long heartbeatTimeoutMs) {
        this.clients = clients;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "heartbeat-scheduler"));
        this.pingPool = Executors.newCachedThreadPool(r -> daemon(r, "heartbeat-ping"));
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** Starts polling every {@code intervalMs}. Safe to call once; call {@link #close()} to stop. */
    public void start(long intervalMs) {
        pollingTask = scheduler.scheduleAtFixedRate(this::pingAllOnce, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    /** Pings every node once, in parallel, and updates last-seen timestamps for the ones that answer. */
    public void pingAllOnce() {
        for (Map.Entry<String, NodeClient> entry : clients.entrySet()) {
            String nodeId = entry.getKey();
            NodeClient client = entry.getValue();
            pingPool.submit(() -> {
                try {
                    client.heartbeat();
                    lastSeenAt.put(nodeId, System.currentTimeMillis());
                } catch (Exception unreachable) {
                    // no update -- the node's timestamp ages out and isAlive() will report false
                }
            });
        }
    }

    public boolean isAlive(String nodeId) {
        Long seenAt = lastSeenAt.get(nodeId);
        return seenAt != null && (System.currentTimeMillis() - seenAt) < heartbeatTimeoutMs;
    }

    @Override
    public void close() {
        if (pollingTask != null) {
            pollingTask.cancel(true);
        }
        scheduler.shutdownNow();
        pingPool.shutdownNow();
    }
}
