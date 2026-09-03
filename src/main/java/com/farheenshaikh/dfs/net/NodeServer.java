package com.farheenshaikh.dfs.net;

import com.farheenshaikh.dfs.ChecksumUtil;
import com.farheenshaikh.dfs.StorageNode;
import com.farheenshaikh.dfs.proto.DfsProto.CorruptChunkRequest;
import com.farheenshaikh.dfs.proto.DfsProto.CorruptChunkResponse;
import com.farheenshaikh.dfs.proto.DfsProto.HeartbeatRequest;
import com.farheenshaikh.dfs.proto.DfsProto.HeartbeatResponse;
import com.farheenshaikh.dfs.proto.DfsProto.NodeRequest;
import com.farheenshaikh.dfs.proto.DfsProto.NodeResponse;
import com.farheenshaikh.dfs.proto.DfsProto.ReadChunkRequest;
import com.farheenshaikh.dfs.proto.DfsProto.ReadChunkResponse;
import com.farheenshaikh.dfs.proto.DfsProto.StoreChunkRequest;
import com.farheenshaikh.dfs.proto.DfsProto.StoreChunkResponse;
import com.google.protobuf.ByteString;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * A real TCP server for one storage node: binds a port, accepts connections, and answers
 * {@code NodeRequest}s by delegating to a local {@link StorageNode}. Every request/response
 * crosses the socket as a length-delimited, binary-encoded Protocol Buffers message
 * ({@link com.google.protobuf.Message#writeDelimitedTo} / {@code parseDelimitedFrom}) --
 * this is the "efficient inter-node communication" layer: compact binary framing over a
 * plain socket, no text parsing, no reflection-heavy serialization.
 */
public class NodeServer implements AutoCloseable {

    private final StorageNode storageNode;
    private final ServerSocket serverSocket;
    private final ExecutorService connectionPool;
    private volatile boolean running = true;

    public NodeServer(String nodeId, int port) throws IOException {
        this.storageNode = new StorageNode(nodeId);
        this.serverSocket = new ServerSocket(port);
        this.connectionPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "node-server-" + nodeId);
            t.setDaemon(true);
            return t;
        });
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    public String nodeId() {
        return storageNode.id;
    }

    /** Exposed for the demo/test harness, which simulates disk corruption locally rather than over RPC. */
    public StorageNode storageNode() {
        return storageNode;
    }

    /** Starts a background thread accepting connections; returns immediately. */
    public void start() {
        Thread acceptThread = new Thread(this::acceptLoop, "node-server-accept-" + storageNode.id);
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                connectionPool.submit(() -> handleConnection(socket));
            } catch (IOException | RejectedExecutionException e) {
                // A RejectedExecutionException means accept() won the race against close():
                // it returned a connection just as the pool was shutting down. Either way, if
                // we're no longer running this is just shutdown in progress -- exit quietly.
                if (running) {
                    System.err.println("[" + storageNode.id + "] accept failed: " + e.getMessage());
                }
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (Socket s = socket;
             InputStream in = new BufferedInputStream(s.getInputStream());
             OutputStream out = new BufferedOutputStream(s.getOutputStream())) {
            NodeRequest request;
            while ((request = NodeRequest.parseDelimitedFrom(in)) != null) {
                NodeResponse response = handle(request);
                response.writeDelimitedTo(out);
                out.flush();
            }
        } catch (IOException e) {
            // client disconnected mid-request; nothing to do
        }
    }

    private NodeResponse handle(NodeRequest request) {
        switch (request.getPayloadCase()) {
            case STORE:
                return NodeResponse.newBuilder().setStore(handleStore(request.getStore())).build();
            case READ:
                return NodeResponse.newBuilder().setRead(handleRead(request.getRead())).build();
            case HEARTBEAT:
                return NodeResponse.newBuilder().setHeartbeat(handleHeartbeat(request.getHeartbeat())).build();
            case CORRUPT:
                return NodeResponse.newBuilder().setCorrupt(handleCorrupt(request.getCorrupt())).build();
            default:
                throw new IllegalArgumentException("empty NodeRequest payload");
        }
    }

    private StoreChunkResponse handleStore(StoreChunkRequest req) {
        byte[] data = req.getData().toByteArray();
        storageNode.store(req.getChunkId(), data);
        return StoreChunkResponse.newBuilder()
                .setSuccess(true)
                .setChecksum(ChecksumUtil.sha256(data))
                .build();
    }

    private ReadChunkResponse handleRead(ReadChunkRequest req) {
        try {
            byte[] data = storageNode.read(req.getChunkId());
            if (data == null) {
                return ReadChunkResponse.newBuilder().setStatus(ReadChunkResponse.Status.NOT_FOUND).build();
            }
            return ReadChunkResponse.newBuilder()
                    .setStatus(ReadChunkResponse.Status.OK)
                    .setData(ByteString.copyFrom(data))
                    .build();
        } catch (IOException corrupt) {
            return ReadChunkResponse.newBuilder().setStatus(ReadChunkResponse.Status.CORRUPTED).build();
        }
    }

    private HeartbeatResponse handleHeartbeat(HeartbeatRequest req) {
        return HeartbeatResponse.newBuilder()
                .setNodeId(storageNode.id)
                .setServerTimeMillis(System.currentTimeMillis())
                .build();
    }

    private CorruptChunkResponse handleCorrupt(CorruptChunkRequest req) {
        return CorruptChunkResponse.newBuilder().setSuccess(storageNode.corrupt(req.getChunkId())).build();
    }

    @Override
    public void close() {
        running = false;
        connectionPool.shutdownNow();
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // best-effort shutdown
        }
    }
}
