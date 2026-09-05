# Distributed File System

A distributed file system in Java: real, independently running storage node
servers that talk to each other over TCP sockets using **Protocol Buffers**,
coordinated by a controller that implements four core building blocks of
real storage systems:

- **Chunked storage with round-robin partitioning** across storage nodes
- **Heartbeat-based failure detection**, driven by real RPCs over the network
- **Poll-driven re-replication** that maintains a user-defined replication factor
- **SHA-256 checksum-based corruption detection and recovery**

Every node in the cluster is a genuine `NodeServer` bound to its own TCP port,
reachable only through Protobuf-encoded requests -- there's no in-process
shortcut. The demo starts 24 of them, writes a file across the cluster in
parallel, kills one node's server outright and corrupts a chunk on another
via RPC, and shows the cluster detect and heal both failures on its own.

