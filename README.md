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

```
put/getFile ──▶ FileChunker ──▶ Controller ──▶ NodeClient ──(TCP + Protobuf)──▶ NodeServer ──▶ StorageNode
                (parallel,        (placement,      (RPC stub,                    (socket          (chunk bytes +
                 per-chunk          replication,     one connection               server,           SHA-256
                 fan-out)           healing,          per call)                   thread pool)       checksum)
                                    heartbeat
                                    polling)
```

## Architecture

- **`dfs.proto`** -- the wire schema: `NodeRequest`/`NodeResponse` envelopes
  (each a `oneof` over four RPCs) carrying `StoreChunkRequest/Response`,
  `ReadChunkRequest/Response`, `HeartbeatRequest/Response`, and a
  `CorruptChunkRequest/Response` test/demo hook for injecting corruption
  over the wire instead of reaching into a node directly. The generated
  Java (`DfsProto.java`) is committed under `src/main/java` so the project
  builds with nothing but a JDK, Maven, and the `protobuf-java` runtime --
  regenerating it only matters if you change `dfs.proto` (see below).
- **`NodeServer`** -- a real `ServerSocket` bound to its own port, backed by
  a thread pool. Each connection is read as a length-delimited `NodeRequest`
  (`Message.parseDelimitedFrom`), dispatched to a local `StorageNode`, and
  answered with a length-delimited `NodeResponse` (`writeDelimitedTo`) --
  compact binary framing over a plain socket, no text parsing.
- **`NodeClient`** -- the RPC stub `Controller` uses to talk to one
  `NodeServer`. Every call opens a fresh socket with a connect/read timeout,
  which is what turns a dead or unresponsive node into an `IOException`
  instead of a hang -- the signal the heartbeat monitor and read/write paths
  rely on to detect failure.
- **`StorageNode`** -- the local storage engine embedded inside a
  `NodeServer`: chunk bytes plus a SHA-256 checksum per chunk. `read()`
  re-verifies the checksum every call and throws if the data has been
  corrupted since it was written; `corrupt()` (invoked only via the
  `CorruptChunkRequest` RPC) simulates on-disk corruption for demos/tests.
- **`HeartbeatMonitor`** -- **heartbeat-based failure detection**: on a fixed
  interval, pings every registered node's real `Heartbeat` RPC *in parallel*
  and records the timestamp of each successful reply. A node counts as alive
  if it answered within the timeout window; a node that's down, unreachable,
  or too slow simply ages out.
- **`Controller`** -- the cluster's coordination layer. It never touches
  chunk bytes directly, only through `NodeClient`:
  - `assign(count)` implements **round-robin partitioning**: it ranks the
    currently alive nodes into a sorted ring and hands out `count` of them
    starting from a rotating cursor, so consecutive chunks spread evenly
    across the cluster instead of piling onto the same few nodes.
  - `writeChunk()` stores a chunk on `replicationFactor` nodes **in
    parallel** (one RPC per replica, fanned out on a thread pool) rather
    than one at a time.
  - `readChunk()` **races** RPCs to every alive replica in parallel and
    returns the first one that passes checksum verification, so one slow or
    dead replica doesn't add latency as long as another answers.
  - `replicationSweep()` implements **poll-driven re-replication**: called
    on a timer, it walks every chunk, and for any chunk with fewer alive
    replicas than the target replication factor, reads it from a healthy
    replica over RPC and writes it to a freshly assigned node. If the
    replica it tries to read from turns out to be corrupted, that replica is
    scrubbed from the bookkeeping list on the spot (see **Known limitation**
    below).
- **`FileChunker`** -- splits a file into fixed-size chunks and fans the
  writes out across chunks **in parallel** (one task per chunk on a thread
  pool), and reassembles a file the same way on read -- this is what lets a
  multi-chunk file spread its I/O across many nodes concurrently instead of
  one chunk at a time.
- **`Demo`** -- wires up a 24-node cluster, starts the heartbeat poller and a
  replication-sweep timer, writes a file in parallel, kills one node's
  server and corrupts a chunk on another over RPC, waits for the cluster to
  self-heal, then reads the file back in parallel and confirms it's
  byte-for-byte intact.

## Quickstart

Requires JDK 17+ and Maven.

```bash
git clone <this-repo-url>
cd DistributedFileSystem

mvn test              # run the test suite (starts real local clusters per test)
mvn compile exec:java # run the end-to-end failure/corruption/healing demo
```

Sample demo output:

```
Started 24 storage nodes, each its own TCP server speaking Protobuf-over-socket (e.g. node-1 on port 46353)

Wrote story.txt as 15 chunk(s) in parallel (112 ms), replication factor 3
  story.txt-chunk-0 -> [node-1, node-10, node-11]
  story.txt-chunk-1 -> [node-10, node-11, node-12]
  story.txt-chunk-2 -> [node-11, node-12, node-13]
  ... (12 more chunks)

Killing node-1 outright (its TCP server goes down)...
Corrupting story.txt-chunk-0 on node-10 via RPC...

Waiting for heartbeat timeout + replication sweeps to heal the cluster...

[scrub] checksum mismatch for story.txt-chunk-0 on node node-10 -- dropping replica
[replication] healed story.txt-chunk-0 -> node-24 (now 3/3 replicas)
[replication] healed story.txt-chunk-0 -> node-3 (now 4/3 replicas)

Read story.txt back in parallel (31 ms)
Recovered file matches original: true
```

## Running the containerized cluster

The demo above runs 24 nodes as threads inside one JVM. `NodeServerMain` and `ControllerMain`
(`src/main/java/com/farheenshaikh/dfs/`) give the same `NodeServer` / `Controller` classes a
process of their own instead, so the cluster can run as genuinely separate OS processes --
containers, in this case, one per node plus one for the controller, talking to each other only
over TCP + Protobuf.

```bash
cd infra
docker compose up --build
```

This builds one image (root `Dockerfile`) and starts it five times as storage nodes plus once as
the controller (`infra/docker-compose.yml` picks the role per service via `command`). The
controller logs a smoke test on startup -- write a file across the real cluster, read it back,
confirm it round-trips -- then keeps logging cluster health on an interval:

```bash
docker compose logs -f controller
```

Node count here is smaller than the 24-node in-process demo on purpose (five containers is
plenty to demonstrate real multi-process placement/heartbeat/healing on a laptop); the mechanism
is identical either way, since `NodeServer` has always been a real bound TCP socket, in-process
or not.

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

- `ChecksumUtilTest`, `StorageNodeTest` -- checksum correctness and
  corruption detection at the local-storage level, no networking involved.
- `FileChunkerTest` -- parallel chunking and reassembly round-trip files
  exactly, including a file spanning many chunks across many nodes, and a
  clean failure when a chunk has no healthy replica left.
- `ControllerTest` -- round-robin placement, replication-factor enforcement,
  read failover across a replica corrupted over RPC, and replication-sweep
  healing under a **real** compound failure: one node's server killed
  outright, another's chunk corrupted via RPC (see below).

## Known limitation: healing after simultaneous failure + corruption

`replicationSweep()` computes how many replacement replicas a chunk needs
*before* it tries reading from a candidate source. If that source turns out
to be corrupted, its replica is dropped -- but the deficit for that sweep was
already computed against the old (higher) alive-replica count, so the sweep
only closes part of the gap. Concretely: if a chunk loses one replica to node
failure and a second replica (of the two remaining) turns out to be corrupt,
one sweep cycle restores the replication factor from 1 healthy copy to 2, not
3 -- a second sweep cycle is needed to fully recover. `ControllerTest`
documents this exact behavior (`fullyRestoringReplicationAfterCompoundFailureCanTakeTwoSweeps`)
rather than hiding it.

This doesn't affect read correctness -- `readChunk()` independently races
every alive replica and skips corrupted ones, so a file stays readable
throughout healing as long as at least one good replica exists. It does mean
the system is *eventually* consistent about restoring full redundancy after a
compound failure, across sweep cycles, rather than within a single one. A
straightforward fix would be to recompute the deficit after the source search
completes (i.e., after any corrupt replicas found along the way have been
dropped) instead of before.

## Known limitation: containerized packaging not verified end to end in this sandbox

`NodeServerMain` and `ControllerMain` were compiled and run for real in this environment as three
separate OS processes on localhost (`javac`/`java`, not Maven, since this sandbox's network
blocks Maven Central) -- a controller process registered three independent node processes purely
by `host:port`, wrote a file across them over real TCP + Protobuf, and read it back byte-for-byte
correct, exactly like the "Running the containerized cluster" walkthrough above but without the
container layer. What that leaves unverified is the packaging around it: the `maven-shade-plugin`
build the `Dockerfile` depends on couldn't run here (same Maven Central block), and there was no
Docker daemon available to actually run `docker build` / `docker compose up`. Run `cd infra &&
docker compose up --build` yourself before relying on the image, or let the CI workflow's
`docker` job do it on push.

## Possible extensions

- Persist chunks to disk instead of an in-memory `Map` inside `StorageNode`.
- Reuse (pool) `NodeClient` connections instead of opening a fresh socket per
  RPC.
- Proactively prune node ids that have been dead for longer than a grace
  period from a chunk's replica bookkeeping, rather than only removing ids
  discovered to be corrupt (see the known limitation above).
- Recompute the sweep's deficit after scrubbing a corrupt source, so a single
  sweep can fully heal a compound failure.
