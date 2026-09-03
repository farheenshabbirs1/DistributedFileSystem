# Distributed File System

A compact, single-purpose distributed file system in Java that demonstrates
four core building blocks of real storage systems:

- **Chunked storage with round-robin partitioning** across storage nodes
- **Heartbeat-based failure detection**
- **Poll-driven re-replication** that maintains a user-defined replication factor
- **SHA-256 checksum-based corruption detection and recovery**

It's intentionally small (no networking -- nodes are in-process objects) so the
coordination logic is easy to read end to end, while still behaving like a
real cluster: nodes fail, disks corrupt, and the system detects and heals both.

```
put/getFile ──▶ FileChunker ──▶ Controller ──▶ StorageNode (×N)
                (fixed-size      (placement,      (chunk bytes +
                 chunking)        replication,      SHA-256 checksum,
                                  healing)           liveness)
```

## Architecture

- **`StorageNode`** -- holds chunk bytes plus a SHA-256 checksum per chunk,
  and tracks its own liveness via a `heartbeat()` timestamp. `read()`
  re-verifies the checksum on every call and throws if the data has been
  corrupted since it was written.
- **`Controller`** -- the cluster's metadata and coordination layer. It
  never touches chunk bytes directly:
  - `assign(count)` implements **round-robin partitioning**: it ranks the
    currently alive nodes into a sorted ring and hands out `count` of them
    starting from a rotating cursor, so consecutive chunks spread evenly
    across the cluster instead of piling onto the same few nodes.
  - `aliveNodes()` implements **heartbeat-based failure detection**: a node
    is "alive" if it heartbeated within the configured timeout window.
  - `replicationSweep()` implements **poll-driven re-replication**: called
    on a timer, it walks every chunk, and for any chunk with fewer alive
    replicas than the target replication factor, copies it from a healthy
    replica onto a freshly assigned node. If the replica it tries to copy
    from turns out to be corrupt, that replica is scrubbed from the
    bookkeeping list on the spot (see **Known limitation** below).
  - `readChunk()` transparently tries every replica of a chunk in order,
    skipping dead nodes and corrupted copies, until one passes checksum
    verification.
- **`FileChunker`** -- splits a file into fixed-size chunks for `Controller.writeChunk`,
  and reassembles a file from its chunk ids via `Controller.readChunk`.
- **`Demo`** -- wires everything together: a 6-node cluster, a background
  heartbeat thread, a background replication-sweep thread, writes a file,
  kills one node and corrupts a chunk on another, waits for the cluster to
  self-heal, then reads the file back and confirms it's byte-for-byte intact.

## Quickstart

Requires JDK 17+ and Maven.

```bash
git clone <this-repo-url>
cd DistributedFileSystem

mvn test              # run the unit test suite
mvn compile exec:java # run the end-to-end failure/corruption/healing demo
```

Sample demo output:

```
Wrote story.txt as 8 chunk(s), replication factor 3
  story.txt-chunk-0 -> [node-1, node-2, node-3]
  story.txt-chunk-1 -> [node-2, node-3, node-4]
  ...

Simulating node-2 failure (heartbeats stop)...
Simulating on-disk corruption of story.txt-chunk-0 on node-1...

Waiting for heartbeat timeout + replication sweep to heal the cluster...

[scrub] checksum mismatch for story.txt-chunk-0 on node node-1 -- dropping replica
[replication] healed story.txt-chunk-0 -> node-1 (now 3/3 replicas)
...

Recovered file matches original: true
```

## Testing

```bash
mvn test
```

`src/test/java` covers each layer:

- `ChecksumUtilTest`, `StorageNodeTest` -- checksum correctness, corruption
  detection, and heartbeat-based liveness at the single-node level.
- `FileChunkerTest` -- chunking and reassembly round-trip files exactly,
  including files spanning multiple chunks.
- `ControllerTest` -- round-robin placement, replication-factor enforcement,
  read failover across corrupted/dead replicas, and the replication-sweep
  healing behavior under combined node failure + corruption (see below).

## Known limitation: healing after simultaneous failure + corruption

`replicationSweep()` computes how many replacement replicas a chunk needs
*before* it tries reading from a candidate source. If that source turns out
to be corrupted, its replica is dropped -- but the deficit for that sweep was
already computed against the old (higher) alive-replica count, so the sweep
only closes part of the gap. Concretely: if a chunk loses one replica to node
failure and a second replica (of the two remaining) turns out to be corrupt,
one sweep cycle restores the replication factor from 1 healthy copy to 2, not
3 -- a second sweep cycle is needed to fully recover. `ControllerTest`
documents this exact behavior (`fullyRestoringReplicationAfterCorruptionAndStalenessCanTakeTwoSweeps`)
rather than hiding it.

This doesn't affect read correctness -- `readChunk()` independently tries
every replica and skips dead or corrupted ones, so a file stays readable
throughout healing as long as at least one good replica exists. It does mean
the system is *eventually* consistent about restoring full redundancy after a
compound failure, across sweep cycles, rather than within a single one. A
straightforward fix would be to recompute the deficit after the source search
completes (i.e., after any corrupt replicas found along the way have been
dropped) instead of before.

## Possible extensions

- Replace in-process `StorageNode` objects with real networked services (gRPC/HTTP).
- Persist chunks to disk instead of an in-memory `Map`.
- Proactively prune node ids that have been dead for longer than a grace
  period from a chunk's replica bookkeeping, rather than only removing ids
  discovered to be corrupt (see the known limitation above).
- Recompute the sweep's deficit after scrubbing a corrupt source, so a single
  sweep can fully heal a compound failure.
