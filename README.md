# Distributed File System

A distributed file system in Java: real, independently running storage node
servers that talk to each other over TCP sockets using **Protocol Buffers**,
coordinated by a controller that implements four core building blocks of
real storage systems:

- **Chunked storage with round-robin partitioning** across storage nodes
- **Heartbeat-based failure detection**, driven by real RPCs over the network
- **Poll-driven re-replication** that maintains a user-defined replication factor
- **SHA-256 checksum-based corruption detection and recovery**

## Quickstart

Requires JDK 17+ and Maven.

```bash
git clone <this-repo-url>
cd DistributedFileSystem

mvn test              # run the test suite (starts real local clusters per test)
mvn compile exec:java # run the end-to-end failure/corruption/healing demo
```



## Regenerating the Protobuf sources

Only needed if you change `src/main/proto/dfs.proto`. Requires the `protoc`
compiler (matching the `protobuf.version` in `pom.xml`, currently 3.21.x):

```bash
protoc --java_out=src/main/java src/main/proto/dfs.proto
```

## Testing

```bash
mvn test
```

`src/test/java` covers each layer. `TestCluster` is a shared fixture that
starts a real, small cluster of `NodeServer`s on OS-assigned free ports for
each test -- there's no mocked network layer.
