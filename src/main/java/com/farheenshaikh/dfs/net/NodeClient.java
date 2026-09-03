package com.farheenshaikh.dfs.net;

import com.farheenshaikh.dfs.proto.DfsProto.CorruptChunkRequest;
import com.farheenshaikh.dfs.proto.DfsProto.HeartbeatRequest;
import com.farheenshaikh.dfs.proto.DfsProto.HeartbeatResponse;
import com.farheenshaikh.dfs.proto.DfsProto.NodeRequest;
import com.farheenshaikh.dfs.proto.DfsProto.NodeResponse;
import com.farheenshaikh.dfs.proto.DfsProto.ReadChunkRequest;
import com.farheenshaikh.dfs.proto.DfsProto.ReadChunkResponse;
import com.farheenshaikh.dfs.proto.DfsProto.StoreChunkRequest;
import com.farheenshaikh.dfs.proto.DfsProto.StoreChunkResponse;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * RPC stub for talking to one remote {@link NodeServer}. Every call opens a fresh socket,
 * sends one length-delimited Protobuf {@code NodeRequest}, reads back one {@code NodeResponse},
 * and closes the connection -- trading a little per-call connection overhead for simplicity and
 * thread safety (callers may invoke a NodeClient from several threads concurrently, e.g. during
 * parallel replica writes, with no shared mutable connection state to coordinate).
 *
 * <p>A connect/read timeout is what makes a dead or unresponsive node observable as a failure
 * (an {@link IOException}) rather than a hang -- this is what the heartbeat-based failure
 * detector in {@code Controller} relies on.
 */
public class NodeClient {

    private final NodeEndpoint endpoint;
    private final int timeoutMs;

    public NodeClient(NodeEndpoint endpoint, int timeoutMs) {
        this.endpoint = endpoint;
        this.timeoutMs = timeoutMs;
    }

    public NodeEndpoint endpoint() {
        return endpoint;
    }

    public StoreChunkResponse storeChunk(String chunkId, byte[] data) throws IOException {
        NodeRequest request = NodeRequest.newBuilder()
                .setStore(StoreChunkRequest.newBuilder()
                        .setChunkId(chunkId)
                        .setData(ByteString.copyFrom(data))
                        .build())
                .build();
        return call(request).getStore();
    }

    public ReadChunkResponse readChunk(String chunkId) throws IOException {
        NodeRequest request = NodeRequest.newBuilder()
                .setRead(ReadChunkRequest.newBuilder().setChunkId(chunkId).build())
                .build();
        return call(request).getRead();
    }

    public HeartbeatResponse heartbeat() throws IOException {
        NodeRequest request = NodeRequest.newBuilder()
                .setHeartbeat(HeartbeatRequest.newBuilder().setRequesterId("controller").build())
                .build();
        return call(request).getHeartbeat();
    }

    /** Test/demo-only: asks the node to simulate corruption of one of its stored chunks. */
    public boolean corruptChunk(String chunkId) throws IOException {
        NodeRequest request = NodeRequest.newBuilder()
                .setCorrupt(CorruptChunkRequest.newBuilder().setChunkId(chunkId).build())
                .build();
        return call(request).getCorrupt().getSuccess();
    }

    private NodeResponse call(NodeRequest request) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(endpoint.host, endpoint.port), timeoutMs);
            socket.setSoTimeout(timeoutMs);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            request.writeDelimitedTo(out);
            out.flush();

            NodeResponse response = NodeResponse.parseDelimitedFrom(in);
            if (response == null) {
                throw new IOException("node " + endpoint.id + " closed the connection with no response");
            }
            return response;
        }
    }
}
