# Ownership and fencing

celld makes two claims. One node owns a cell at a time, and a write is
durable before celld acknowledges it. This page gives the mechanism
behind each claim, so you can check the argument against the code.

A short version first. The ownership records use conditional writes.
The replication stream uses plain writes, because the epoch in the key
is the fence. The acknowledgement path re-reads the ownership record,
so a stale node cannot make a promise that the fleet does not keep.

## The ownership record

Each cell has one ownership record in the bucket. The record names the
owner node's session and carries a fencing epoch. A node acquires a
cell with a conditional write: a create when no record exists, and a
compare-and-swap on the previous record when one does. The bucket
accepts one such write, so two nodes cannot acquire the same cell.

Every activation advances the epoch. A takeover advances it, and a
local wake advances it too. Each owner therefore replicates under a
fresh epoch, and an epoch never has two writers.

## Replication and the epoch prefix

The replicator copies the SQLite data of each cell to the bucket under
an epoch prefix: `cells/<cell>/ltx/e<epoch>/`. These segment writes
are plain, unconditional PUTs. That is intentional: the fence is the
epoch in the key, not a condition on the request. A node that lost its
ownership can continue to write, but its writes land in a superseded
prefix. A restore selects the current lineage, so the stale node
cannot corrupt the data of the new owner, and the data path pays no
conditional-write cost.

The prefix protects the data. It does not, alone, protect the promise
to the client. The next two mechanisms close that gap.

## The acknowledgement rule (RPO=0)

A gate holds the response of each write until the replicator proves
that the write is in the bucket. After the proof, celld reads the
ownership record one time, and celld acknowledges only if the record
still names this node at this epoch. A partitioned node can commit
locally and replicate into its own superseded prefix, but its
ownership read shows the new owner, so the client never receives an
acknowledgement for a write that the surviving lineage does not
contain. The check is a read of the record, not a clock comparison, so
a paused process or a skewed clock cannot pass it.

## The epoch seal

A restore selects the newest epoch prefix that contains data. A fenced
node can append to that prefix after the takeover, because the segment
writes are unconditional. Without a further rule, a later restore
could read that appended tail: writes that no client saw acknowledged.

The seal closes this hole. The first activation that restores from an
epoch writes a seal object (`e<epoch>.seal.json`) with a conditional
create, and the seal fixes the highest transaction that any restore of
that epoch can read. The conditional create means that the first
restorer wins, so every later restore reads the same cut. Every
acknowledged write sits at or below the seal, because the
acknowledgement required an ownership read, and the takeover preceded
the seal. The later writes of the fenced node sit above the seal, so
they never return. If the seal write fails, the activation fails,
because a restore of an unsealed prefix reopens the hole.

## Self-fencing

A node that cannot reach the bucket cannot renew its lease, and it
cannot replicate. Such a node must not own cells, so it fences itself:
it stops the writes and releases its residency. A different node can
then acquire the cells through the ownership records. The failure of a
node is a normal input, not a recovery procedure; the [testing
page](testing.md) shows the kill tests that exercise this path.

## What the bucket must provide

celld needs three properties from the object store:

- A conditional create. The create must fail when the object already
  exists.
- A conditional overwrite. The write must fail when the object changed
  after the read.
- Read-after-write consistency. A read after a successful write must
  return that write.

On an S3-compatible bucket, celld sends the `If-None-Match: *` and
`If-Match` headers, and the condition compares the etag. Amazon S3,
Cloudflare R2, and Tigris document these operations. celld's release
tests run against Cloudflare R2, and the AWS S3 path uses the same
client and the same headers.

A `gs://` bucket selects Google Cloud Storage. celld then uses the
Cloud Storage XML API with the `x-goog-if-generation-match`
precondition and OAuth credentials, and the condition compares the
object generation. celld does not send the S3 request dialect to
Cloud Storage, because Cloud Storage does not apply `If-Match` to a
PUT.

Some S3-compatible stores do not qualify. MinIO (the community
edition), Backblaze B2, Hetzner Object Storage, and DigitalOcean
Spaces do not implement the required conditional writes. celld is not
correct on such a store: two nodes can then own one cell. A store can
also accept the conditional headers and not apply the condition, and
that store fails late and silently.

## The storage test

No object store publishes the answer, therefore celld asks the store
directly. The command `celld diagnose` sends four conditional writes to
your bucket, and it reports the result:

```
ok bucket conditional write (create, reject-create, update, reject-stale)
```

Two of the four writes must fail. A create over an existing object must
fail, and an update that carries a stale token must fail. A store that
applies either write cannot fence a cell, so celld names the store as
the fault and the command exits with an error.

Each node runs the same test one time at startup. A node that finds a
broken store stops, because a node that serves on such a store can
share a cell with a second owner. The value `0` in the
`CELLD_STORAGE_PROBE` variable disables the startup test.

The test writes one small object, and then it deletes the object. The
object uses the `probe/` prefix. celld reserves that prefix together
with `cells/`, `nodes/`, `node-cells/`, `fleet/`, `deploy/`,
`deploy-blobs/`, `wake/` and `telemetry/`, so an application must not
write under any of them. celld deletes objects under some of them. A
process that stops during the test does not delete the object. The
object is small, and celld never reads it.

The test writes, so a credential that cannot write fails it. An
operator who diagnoses with a read-only credential runs
`celld diagnose --read-only`, and celld then skips the test.

celld does not require a ranged read today, so a store that ignores the
`Range` header can still run a fleet. A later compaction level can hold
large snapshots, and such a level can need a ranged read again.
