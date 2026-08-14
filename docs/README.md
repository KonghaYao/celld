# celld documentation

celld is a stateful distributed system. It runs server-side JavaScript on
your machines. It keeps all shared data in an S3-compatible or Google
Cloud Storage bucket that you own. The JavaScript API is the same API
that Cloudflare Workers and Durable Objects supply.

In Cloudflare terms, a cell is a Durable Object: a small server with a
name and a private SQLite database. You make one cell for each user, each
document, each chat room, or each AI agent. A cell serves HTTP, holds
WebSocket connections, sets alarms, and makes outbound connections. Each
cell runs on one thread, so two requests to the same cell never run at
the same instant. A second request can interleave only while the first
awaits, and storage operations are synchronous, so a storage operation
never interleaves at all. The data in a cell therefore stays consistent.
Cells share no database, and the application divides into cells from the
start.

A cell has the same states as a Durable Object. A **resident** cell is in
memory: it is **active** while it does work, and **idle** when it waits.
celld removes an idle cell from memory. A cell that then keeps its
hibernatable WebSocket clients, and stays on its node, is **hibernated**.
A cell that no node holds is **inactive**. An inactive cell is only an
object in the bucket, so it costs almost zero, and every cell starts in
this state.

Memory holds nothing across these transitions, so the constructor runs
again on the next event. A hibernated cell therefore starts as a cold
start does, and only two facts separate the two states: the WebSocket
clients stay connected, and the cell stays on its node.

One 8 GB node holds 1,000 resident cells, so one resident cell costs
approximately $0.05 each month.

The bucket is the coordinator. There is no membership protocol, no
failure detector, and no consensus service. One atomic write to the
bucket gives a node the ownership of a cell. Replication sends the SQLite
data of each cell to the bucket, and celld does not acknowledge a write
before the data is there (RPO=0). The loss of a node therefore cannot
lose an acknowledged write. To add a node to the fleet, point the node at
the bucket. The [ownership and fencing](fencing.md) page gives the full
mechanism and the exact properties that the bucket must provide.

## What do you build with cells

A cell fits a workload that divides into named, stateful units:

- **Real-time applications.** A multiplayer game, a chat room, or a
  collaborative document is one cell. The cell holds the WebSocket
  connections and the state of one room, so the room needs no lock and
  no external message bus.
- **Agents.** Each AI agent is one cell. The cell holds the memory, the
  schedule, and the inbox of one agent in its own SQLite database, and
  an idle agent hibernates to the bucket, so a large agent fleet costs
  almost nothing between events.
- **Sharded web applications.** One cell for each user, each tenant, or
  each device shards the application from the start. The contention of
  one shared database does not appear, because no shared database
  exists.

## Contents

- [What do you build with cells](#what-do-you-build-with-cells)
- [Install](#install)
- [Configure object storage](#configure-object-storage)
- [Deploy an application](#deploy-an-application)
- [Start a node](#start-a-node)
- [Add nodes](#add-nodes)
- [Diagnose a fleet](#diagnose-a-fleet)
- [Environment variables](#environment-variables)
- [Cloudflare compatibility](cloudflare-compat.md)
- [Ownership and fencing](fencing.md)
- [limitations](limitations.md)
- [Security](security.md)
- [Telemetry](telemetry.md)
- [Testing](testing.md)
- [WebAssembly](wasm.md)

## Install

The installer downloads the `celld` binary. Replication occurs in the
celld process. A node does not need an external replicator. If your
project contains Worker code, `celld deploy` needs esbuild. An asset-only
project does not need esbuild.

```sh
curl -fsSL https://celld.dev/install.sh | sh
```

If the installer tells you, add `~/.local/bin` to `PATH`. To install one
exact release, set `CELLD_VERSION` to the tag of that release, for example
`v0.0.1`. To go back to a previous release, run the installer again with
the tag of that release. The releases are on
[GitHub](https://github.com/denoland/celld/releases). Each release has a
GitHub Actions build attestation. To make sure that a downloaded file is
correct, run `gh attestation verify <asset> --repo denoland/celld`.

## Configure object storage

For an S3-compatible bucket, celld uses the standard AWS credential
chain. For Cloudflare R2, do these steps. Create a bucket. Create an S3
API token that has access to that bucket. Then set these variables:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=auto
export S3_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com
export CELLD_BUCKET=s3://YOUR-BUCKET
```

For Google Cloud Storage, celld uses Application Default Credentials.
Create a bucket. Then authenticate with `gcloud auth application-default
login`, or point `GOOGLE_APPLICATION_CREDENTIALS` at a service-account
key that has access to the bucket. Then set the bucket:

```sh
export CELLD_BUCKET=gs://YOUR-BUCKET
```

A `gs://` bucket takes no `S3_ENDPOINT` and no `AWS_*` credentials, and
celld ignores the storage region.

On a Compute Engine instance, celld can use the attached service
account. The access scopes of the instance cap this credential, and the
default scope permits only storage reads. Create the instance with the
`cloud-platform` scope, so the IAM role of the service account controls
the access.

The bucket credentials give full control of the fleet. Keep them safe. The
bucket contains the deployments, the SQLite replicas, the ownership
records, the node leases, and the peer-authentication secret.

A bucket value can add a key prefix: `s3://YOUR-BUCKET/PREFIX`. Every
object of the fleet then goes below `PREFIX/`, so two fleets can share one
bucket. A bucket value without a prefix keeps the objects at the root of
the bucket, therefore an existing fleet does not move its data.

The store must provide conditional writes and read-after-write
consistency, because the ownership records depend on them. Amazon S3,
Cloudflare R2, Google Cloud Storage, Azure Blob Storage, and Tigris
qualify; MinIO (community edition), Backblaze B2, Hetzner, and
DigitalOcean Spaces do not. See [ownership and fencing](fencing.md)
for the exact requirements.

## Deploy an application

If the project contains Worker code, install `esbuild` on `PATH`. Then run
`celld deploy` from an applicable Wrangler project:

```sh
git clone https://github.com/denoland/celld
cd celld/examples/counter
celld deploy . \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

`celld deploy` accepts module Workers, Durable Object bindings, and static
assets. An asset project can include a Worker or be asset-only. The asset
functions include the assets binding, HTML handling, not-found handling,
worker-first routes, `_headers`, and `_redirects`. If the Wrangler
configuration contains an unknown key, the deploy stops with an error. See
the [limitations](limitations.md) for the current deployment boundary.

## Start a node

For local development, the default listener is sufficient:

```sh
celld \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

For a fleet node, bind the public and internal listeners separately. The
ingress can reach the public listener, and the other nodes can reach the
internal listener:

```sh
celld \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION" \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise node-a.internal:8081
```

An explicit advertised address requires an explicit internal-listener address.
Set both command-line options, or use their equivalent environment variables.
celld also rejects an explicit non-loopback public listener without an explicit
internal listener. This rule identifies an obsolete one-listener configuration.

celld cannot verify that an advertised hostname or a translated port reaches
the internal listener. You must route the advertised address to the internal
listener, and you must not route it to the public Worker listener.

## Add nodes

Start each node with the same bucket settings. Give each internal listener a
different address that the other nodes can reach. Set `--advertise` to that
internal address. The nodes find each other through the leases in the bucket.
There is no join command and no fixed membership list.

The bucket supplies discovery and authority. The bucket does not supply
network reachability. The peer HTTP protocol has a version, a body
signature, an HMAC, a clock limit, and replay protection. celld does not
terminate TLS. Put the advertised addresses on a private network that you
trust, or on an encrypted overlay such as WireGuard or Tailscale. The internal
listener also has an unauthenticated operator API, so do not show it to the
public internet. See the [security](security.md) page for the complete boundary.

## Shut down and roll out a node

celld shuts a node down gracefully on SIGTERM or SIGINT, and these are the
signals `systemctl stop`, `docker stop`, and a Kubernetes pod delete send.
The `/__celld/health` path reports the node as unhealthy, so a load balancer
stops routing to it. The node
answers each new request with a 503 and closes the connection, so a client
retries on a healthy node. The node then hands every resident cell to a
peer by releasing its ownership, and it finishes the requests already in
flight. A cell that serves a request is handed off when that request
finishes. A peer takes over each released cell at once, so the node leaves
without the takeover gap of an abrupt kill.

The internal listener continues to accept `/state` requests during the drain.
The response reports the `occupied`, `evicting`, and `restoring` values. The
public health response identifies the active drain with a 503 status.

The handoff runs at most `CELLD_RELEASES` releases at the same time, and
the default is 128. This bound keeps a node with many cells from flooding
the object store at shutdown. `CELLD_SHUTDOWN_DRAIN_MS` bounds the whole
drain, and its default is 25000. The node exits when the handoff and the
in-flight requests finish, or when this many milliseconds pass, whichever
is first — an idle node exits immediately. You must set it below the stop
grace of your orchestrator, such as systemd `TimeoutStopSec` or Kubernetes
`terminationGracePeriod`, so the orchestrator does not send SIGKILL.

To roll out a new version, use the rolling update of your orchestrator:
stop each node with SIGTERM, wait for its replacement to report healthy,
then move to the next node. celld has no rollout command, because the
health signal lets the orchestrator pace the roll.

The upgrade from v0.1.0 to v0.2.0 must not be a rolling update. Stop
every v0.1.0 node, then start the v0.2.0 nodes. Two changes require
this: v0.2.0 nodes advertise the internal listener, so ownership
records that v0.1.0 nodes wrote name an address that v0.1.0 peers can
not follow to a v0.2.0 node; and v0.2.0 compacts replicated data into
block objects that a v0.1.0 reader can not restore. A fleet must not
mix the two versions.

The internal listener also provides an alpha operator API. `/state` reports the
node state, and `POST /shutdown` starts the same graceful handoff. The
`POST /shutdown?handoff=preserve` request prepares a clean same-node reload and
keeps the ownership records. A release can change this API, so keep the
operator tooling and the celld release together.

## Diagnose a fleet

`celld diagnose` reads the node leases in the bucket. Then it sends a
probe to each live peer. It does not get a lease. It does not change
ownership.

```sh
celld diagnose \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

To probe only some nodes, use `--peer NODE_ID` one or more times. The
report identifies expired records, unsafe or incorrect advertised
addresses, peers that it cannot reach, authentication failures, and
protocol versions that do not agree.

Each node line also shows `restoring`. This value counts each cold route that
holds an activation permit or waits for one. A capacity waiter already holds
a permit, so the value counts each cold route once. During a rolling update,
you must wait for every node to report `restoring=0` before you restart the
next node. Therefore, one restart's cold work finishes before the next restart
removes more warm capacity.

## Environment variables

For the full list, run `celld -h`. This table shows the primary settings:

| variable | purpose |
| --- | --- |
| `CELLD_BUCKET` | The fleet bucket, and an optional key prefix. The same as `--bucket` |
| `S3_ENDPOINT` | The S3-compatible endpoint. The same as `--endpoint` |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | The storage region |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Explicit AWS credentials. The standard AWS credential chain is also available |
| `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_SERVICE_ACCOUNT_KEY` | Google credentials for a `gs://` bucket. Application Default Credentials are also available |
| `CELLD_ADDR` | The public Worker listener. The same as `--listen` |
| `CELLD_INTERNAL_ADDR` | The peer and operator listener. The same as `--internal-listen` |
| `CELLD_ADVERTISE` | The internal address that peers can reach. The same as `--advertise` |
| `CELLD_UNSAFE_PUBLIC_ADVERTISE` | Set to `1` to permit a literal public IP in `CELLD_ADVERTISE`. This setting does not resolve a DNS name or restrict the internal listener |
| `CELLD_NODE` | An explicit node-session ID |
| `CELLD_WATCH` | The local work directory for SQLite and replication |
| `CELLD_ESBUILD` | The path of the esbuild executable |
| `CELLD_ACTIVATIONS` | The limit for concurrent cold-cell activations (default: the available CPU count or 128, whichever is smaller) |
| `CELLD_OPERATION_DEADLINE_MS` | The deadline for a non-restore operation (default: 15000) |
| `CELLD_WORKER_LOADER` | Bind a Worker Loader (Code Mode) at this `env` name. A Worker can then start isolates at runtime. Off unless set (experimental) |
| `CELLD_MAX_LOADED_WORKERS` | The limit for concurrent loaded workers (default: 256) |
| `CELLD_MAX_RESIDENT_CELLS` | The hard limit for resident cells, enforced at admission |
| `CELLD_MAX_RSS_MB` | The memory threshold for pressure shedding, applied to the memory that the cells hold (default: 80% of the available memory; 0 disables the threshold and the absolute cap) |
| `CELLD_OUTPUT_GATE` | The default is `1`, so celld proves each write durable before it acknowledges the write. Set `0` to remove the replication wait and accept possible loss of an acknowledged write |
| `CELLD_LTX_COMPACTION` | The default is `1`: celld creates additive L1 objects, and a takeover reads tens of objects instead of thousands. Set `0` on every node of a mixed fleet until all nodes can read v0.5.2 block objects, because an old reader cannot take over a cell after its first L1 publication |
| `CELLD_LTX_COMPACTION_MIN_TXIDS` | The durable TXID distance that queues an L1 attempt (default: 256) |
| `CELLD_LTX_COMPACTIONS` | The node-wide limit for concurrent L1 attempts (default: 2) |
| `CELLD_VAR_*`, `CELLD_VARS_FILE` | Worker variable overrides |
| `RUST_LOG` | The runtime log filter |

The help output also shows the advanced tuning switches and their
defaults.

An unset variable selects its documented default. A Boolean variable accepts
only `0` or `1`. celld exits during startup when a supplied value is invalid.
