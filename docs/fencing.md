# Ownership and fencing

celld makes two claims. One node owns a cell at a time, and a write is
durable before celld acknowledges it. This page gives the mechanism
behind each claim, so you can check the argument against the code.

A short version first. The ownership records use conditional writes.
The bucket replication path uses plain writes, because the epoch in the key
is the fence. After a bucket proof, celld re-reads the ownership record and
acknowledges the write only if the record still names this node. Every
follower must fsync the write for a fleet proof. During a cold activation,
celld recovers the prior node log before it reads the bucket. These paths
stop a stale node from acknowledging a write that the fleet does not store.

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

The replicator copies the SQLite data of each cell to the bucket under an
epoch prefix: `cells/<cell>/ltx/e<epoch>/`. These segment writes are plain,
unconditional PUTs. The tiering path can first combine segments from many
cells into a node bundle, and it later drains each segment into its per-cell
prefix.

The epoch in the key is the fence, so the data path needs no conditional
write. A node that lost ownership can continue to write, but its writes land
in a superseded prefix. A restore selects the current lineage, so the stale
node cannot corrupt the data of the new owner.

The prefix protects the current owner from stale writes. The acknowledgement
rule and the takeover recovery gate protect the durability promise.

## The acknowledgement rule (RPO=0)

A gate holds each write response until a durability proof covers the write.
After a bucket proof, celld reads the ownership record one time. celld
acknowledges only if the record still names this node at this epoch. A
partitioned node can commit locally and replicate into its superseded prefix.
The ownership read then shows the new owner, so celld does not acknowledge the
write. The check reads the record instead of comparing a clock, so a paused
process or a skewed clock cannot pass it.

A fleet proof does not require this read. Every member of the follower
ensemble must fsync the write. A takeover seals the prior node-log session
before it restores, so the stale owner cannot complete another fleet proof.

## The takeover recovery gate

The default fleet mode can acknowledge a write after all members of a node
log ensemble store the write. The bucket upload can complete later. Each
process session therefore creates a conditional node log record before its
first fleet-durable acknowledgement.

A cold activation checks the log records of the prior owner before it reads
the bucket. An absent record proves that the session never acknowledged past
the bucket, and a sealed record proves that recovery completed. An open or
recovering record makes the activation run recovery.

Recovery uses compare-and-swap writes to fence the record. It seals the
reachable followers, gathers their retained segments and bundles, and uploads
the missing segments into the per-cell prefixes. Recovery then marks the log
record as sealed with another compare-and-swap write. The activation cannot
restore until this sequence completes.

## Full-prefix restore

celld no longer writes an epoch seal object. A restore selects the newest
epoch prefix that contains LTX data, and it reads the full contiguous chain
from transaction zero. A legacy `e<epoch>.seal.json` object does not limit
this chain.

A fenced node can append an unacknowledged tail to an older prefix. A later
restore can expose that tail if it selects the prefix. This result does not
violate the acknowledgement contract, because a failed or absent
acknowledgement does not prove that the write is absent.

A node-log recovery or a bundle drain can add an acknowledged follower tail
after an earlier restore. A fixed restore cut stops at the transaction of that
earlier restore, therefore it hides the recovered tail and loses acknowledged
data. celld does not use a fixed restore cut. The full-prefix rule reads the
complete chain, so a later restore returns the recovered tail and the
acknowledgement contract holds.

## Self-fencing

Each node holds a node lease in the bucket. The lease carries an expiry, and
the node renews the lease after one third of the lease lifetime. The
`CELLD_TTL_MS` variable sets that lifetime, and the default is 10000 ms. A
renewal that does not reach the bucket does not fence the node, because the
node retries while the published expiry has not passed.

A node that cannot reach the bucket cannot renew its lease, and it cannot
replicate. Such a node must not own cells, so it fences itself when its
published expiry passes. It stops each active cell, and it fails every
request that it has not completed. A node whose lease record another writer
replaced or removed fences at once, because that record proves the authority
moved.

The fence writes nothing to the bucket. Each peer already reads the lease
record as dead or as replaced, therefore a peer can acquire the cells through
the ownership records. The handover comes from the lease record itself, not
from a release that the fenced node performs.

A request is safe even before the fence runs. celld compares the current time
against the published expiry each time it routes a request, so a node with a
lapsed lease refuses the request. The fence stops the process, and the
dispatch check keeps one owner per cell.

A fenced node logs a line that starts with `SELF-FENCE:`, and then the process
stops with the exit code 3. The line names the cause, because celld reports
other internal failures with the same prefix and the same exit code. The
fenced state is terminal, so the process cannot take a new lease, and only a
restart returns the node to the fleet. A restart uses the same cold activation
path that a peer failure already uses, therefore celld needs no second
recovery procedure.

You must run celld under a supervisor that restarts the process, such as
systemd, Docker with a restart policy, or Kubernetes. A node without such a
supervisor does not return after a fence, and the fleet loses that capacity
until an operator intervenes. The supervisor must restart the process without
an attempt limit, and it must wait at least one lease lifetime between
attempts. A node that cannot acquire a lease at startup retries and does not
exit, so a repeated fence needs a node that acquires a lease and then loses
it, and the wait keeps that cycle slow enough to observe.

The failure of a node is a normal input, not a recovery procedure; the
[testing page](testing.md) shows the kill tests that exercise this path.

## What the bucket must provide

celld needs three properties from the object store:

- A conditional create. The create must fail when the object already
  exists.
- A conditional overwrite. The write must fail when the object changed
  after the read.
- Read-after-write consistency. A read after a successful write must
  return that write.

The bucket adapter treats `AlreadyExists` and `Precondition` as clean
conditional-write rejections. Azure uses `Precondition` for an HTTP 412
response. The adapter keeps every other error ambiguous. A caller must
reconcile an ambiguous error because the write can have changed the object.

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

An `az://` bucket selects Azure Blob Storage, where the NAME is the
container. celld then sends the same `If-None-Match: *` and `If-Match`
headers, and the condition compares the etag, because Put Blob applies
both headers. Microsoft documents these operations.

The qualified stores are Amazon S3, Cloudflare R2, Tigris, Google Cloud
Storage, and Azure Blob Storage.

Azure was qualified on 2026-08-18, against the shipped revision. The
four-step matrix passes under each credential family celld supports —
an account key, a VM managed identity, and an AKS workload identity —
and a node restores a cell from an Azure replica alone after its local
state is destroyed. An LTX file above the 5 MiB threshold, where celld
switches to block-blob multipart, uploads and reads back byte-for-byte;
Azure's own committed block list shows the parts.

What that qualification does not cover: a managed identity on Azure App
Service or Azure Container Apps (see [limitations](limitations.md)),
per-blob write rate limits shaping lease cadence under sustained load,
and multi-node contention. Each run was single-node, and its conditions
are recorded with it.

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
