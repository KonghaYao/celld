# celld

Self-hosted, distributed **Durable Objects**.

celld is an open-source daemon that runs Cloudflare Workers and Durable
Objects on your own machines. Each object is its own SQLite database.
celld addresses an object by name and replicates it to a bucket that you
own. The bucket can be S3-compatible or Google Cloud Storage. The nodes
coordinate through that bucket alone, with no control plane and no
consensus. Because every object is its own small database,
applications shard by construction — the contention and blast-radius failures
of one shared database are designed out, not managed. A cell that no node
holds is inactive, and an inactive cell costs nearly nothing. Learn more at
[celld.dev](https://celld.dev) or read the
[documentation](https://celld.dev/docs).

## How it works

Every `celld` node embeds V8 and executes Wrangler bundles. The fleet shares
one bucket, which contains deployments, cell state, and small ownership
records. The bucket can be S3-compatible or Google Cloud Storage. Object-storage compare-and-swap ensures that exactly one node owns a
cell at a time, without a membership protocol, failure detector, or consensus
service.

celld continuously replicates each cell's SQLite database to the bucket.
When a cell moves, or when an inactive cell activates, its new owner restores
that database and resumes execution. The bucket is the durable source of
truth; nodes are replaceable.

## Install

The installer downloads the `celld` binary (provenance is verifiable with
`gh attestation verify`):

```sh
curl -fsSL https://celld.dev/install.sh | sh
```

Put `~/.local/bin` on your `PATH` if the installer asks you to.

Worker projects deployed with `celld deploy` need
[esbuild](https://esbuild.github.io) on `PATH`; asset-only projects do not.

The installer keeps each release under `~/.local/lib/celld/releases` and points
one symlink at the current one. To remove celld, delete the symlink and the
releases:

```sh
rm `which celld` && rm -rf ~/.local/lib/celld
```

## Container

The release image contains the `celld` binary and is published for Linux
x86-64 and ARM64:

```sh
docker run --rm ghcr.io/denoland/celld --version
```

Persist the runtime's local state and pass the standard AWS credential
environment through:

```sh
docker volume create celld-state
docker run --rm --network host \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  -e CELLD_WATCH=/var/lib/celld/state \
  -v celld-state:/var/lib/celld \
  ghcr.io/denoland/celld \
  --bucket s3://my-cells-bucket \
  --endpoint https://ACCOUNT.r2.cloudflarestorage.com \
  --region auto \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise node-a.internal:8081
```

Drop `--endpoint` and `--region` for AWS S3. Expose port 8080 through the load
balancer, and keep port 8081 on the private network.

## Run it

celld uses the standard AWS credential chain. Deploy to an S3-compatible
bucket, then start celld against the same bucket:

```sh
celld deploy . \
  --bucket s3://my-cells-bucket

celld \
  --bucket s3://my-cells-bucket \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise 10.0.0.12:8081
```

Use `--endpoint` for another S3-compatible service and `--region` when it
cannot be inferred. A `gs://` bucket selects Google Cloud Storage. celld then
uses the Cloud Storage XML API with generation preconditions. Authentication
uses Application Default Credentials. celld rejects an S3 `--endpoint` for a
`gs://` bucket, and it ignores the storage region:

```sh
celld deploy . --bucket gs://my-cells-bucket
celld --bucket gs://my-cells-bucket --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 --advertise 10.0.0.12:8081
```

A fleet runs one application, and every node loads its
latest successfully committed deployment from `deploy/current.json`. Run
`celld --help` for the complete command line.
Deployment objects use the documented types in `crates/celld/protocol.rs`. `celld
deploy` invokes `esbuild` from `PATH` for Worker code, accepts the supported
Wrangler config subset—including co-deployed or asset-only static
assets—and writes those objects directly. Every node discovers owners and
peers from bucket leases; there is no account or join service.

Peer HTTP and the operator API use the internal listener. Put every advertised
address on a trusted private network or an encrypted overlay such as WireGuard
or Tailscale. Do not publish the internal port. celld rejects a literal public
IP unless you supply `--unsafe-public-advertise`. An explicit advertised
address requires an explicit internal-listener address. celld cannot verify a
hostname or a translated port, so you must route the advertised address to the
internal listener. The first current node creates `fleet/peer-auth.json` in the
bucket. All peer requests are
protocol-versioned, body-bound, HMAC-authenticated, clock-bounded, and
replay-protected with that fleet secret. Treat access to the bucket and its
credentials as fleet administrator access.

## Operate a fleet

`celld diagnose` enumerates every node lease by default, then performs a signed
direct probe of each live peer:

```sh
celld diagnose --bucket s3://my-cells-bucket
```

The report keeps checking after an individual failure and distinguishes
expired records, malformed or unsafe advertise addresses, unreachable peers,
and incompatible protocols. It also prints each node's coarse resident-cell,
WebSocket, RSS, CPU, file-descriptor, pressure, and shedding sample. Pass one
or more `--peer NODE_ID` options to restrict the check.

Set a hard resident-cell limit on each loaded node:

```sh
CELLD_MAX_RESIDENT_CELLS=1000 \
celld --bucket s3://my-cells-bucket --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 --advertise node-a.internal:8081
```

celld enables a memory threshold at 80% of the available memory by default. Set
`CELLD_MAX_RSS_MB` to change the threshold, or set it to `0` to disable memory
pressure shedding. celld measures the memory that the cells hold, and not the
resident set size of the process. The two differ, because the memory allocator
keeps some freed pages instead of returning them to the operating system.
Shedding a cell cannot return those pages, so a threshold on the resident set
size holds a node in pressure after the node gives every cell back. The `/state`
route reports both numbers.

celld also applies an absolute cap to the resident set size of the process. The
cap is 95% of the available memory. It protects the node when the allocator
holds memory that shedding cannot return, because the operating system stops a
process that uses more memory than the machine has. The node logs a warning when
this cap applies.

The cap is a share of the machine, and celld does not derive it from the
threshold. A `CELLD_MAX_RSS_MB` at or above 95% of the available memory therefore reaches
the cap. The cap is then the effective limit. The node decides on its resident
set size, and celld reports this at startup. `CELLD_MAX_RSS_MB=0`
disables the threshold and the cap together. When celld cannot read the size of
the available memory, it applies a cap of 125% of an explicit threshold.

Under pressure, celld durably replicates and fences the least-recently used idle
cells. It then publishes the cells as unowned without resetting their epochs.
Those cells become inactive, and celld refuses to reacquire new unowned cells.

Each limit releases separately. The threshold releases when the memory in use
falls to 80% of the threshold. The cap releases when the resident set size falls
to 80% of the cap. A crossing of one limit therefore does not hold the node
against the other.

A spare receives no assignment. It acquires a released cell through the same
bucket protocol when normal traffic reaches it. celld does not shed a cell with
active work or a live host WebSocket.

## Contributions

Pull requests are disabled. Coding agents make it too easy to send a large,
low-context change that costs maintainers more time than it saves. Thoughtful
contributions are welcome; please understand the code, keep the patch focused,
and respect the review time you are asking for.

Send a `git format-patch` attachment to [ry@deno.com](mailto:ry@deno.com).

Contributor License Agreement: By emailing a patch, you certify that you have
the right to submit it and assign to Deno Land Inc. all rights in the patch
that you can assign. Where a right cannot be assigned, you grant Deno Land
Inc. a perpetual, irrevocable,
worldwide, royalty-free, transferable, sublicensable license to use, modify,
combine, relicense, redistribute, or publish the patch, in whole or in part,
with or without attribution.

## License

[Apache-2.0](LICENSE)

See the [limitations](docs/limitations.md) and
[security](docs/security.md) pages before operating a public fleet.
