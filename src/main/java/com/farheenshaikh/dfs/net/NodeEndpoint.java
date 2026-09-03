package com.farheenshaikh.dfs.net;

/** A storage node's network address: who it is, and where to reach it. */
public final class NodeEndpoint {

    public final String id;
    public final String host;
    public final int port;

    public NodeEndpoint(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return id + "@" + host + ":" + port;
    }
}
