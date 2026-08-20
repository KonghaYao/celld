# Limitations

The boundaries of the current alpha:

- A fleet runs one application deployment. There is no multi-tenant
  scheduler, no account service, or managed ingress.
- celld requires an S3-compatible bucket, a Google Cloud Storage bucket,
  or an Azure Blob Storage container, also on a laptop. There is no local
  filesystem mode, so local development needs a real bucket or a local
  store that provides conditional writes (see [ownership and
  fencing](fencing.md)).
- The peer HTTP protocol does not terminate TLS. Run the fleet on a
  private network or an encrypted overlay, and terminate public TLS in an
  ingress proxy.
- The fleet bucket is the administrative authority, so give its
  credentials a narrow scope; celld does not make shared object-store
  credentials safe.
- The bucket credentials come from the `AWS_*` environment or from
  explicit managed credentials, which includes instance metadata and web
  identity tokens. celld does not read `~/.aws` profiles or SSO logins.
  A `gs://` bucket authenticates with Google Application Default
  Credentials or with a service account key from the `GOOGLE_*`
  environment; S3 static credentials do not apply to it. An `az://`
  bucket authenticates with a storage account key, a managed identity, or
  a workload identity on the public Azure cloud. celld requires exactly one
  credential family, and it refuses every recognized Azure configuration
  variable outside those families. It ignores an `AZURE_*` name that
  `object_store` 0.11.2 does not recognize.
- An `az://` bucket reads a managed identity from the Azure instance
  metadata service, which an Azure VM and an AKS node supply. celld does
  not read the `IDENTITY_ENDPOINT` variable, so a managed identity on
  Azure App Service or Azure Container Apps does not work. Use a workload
  identity or a storage account key on those two platforms.
- The [Cloudflare compatibility](cloudflare-compat.md) page shows what
  celld runs of the Workers platform: the available APIs, the deploy
  contract, and what is out of scope (KV, R2, `wrangler.toml`, routes).
  An unknown key or API causes a loud failure; a silent gap is a bug.
- Each node can be the WebSocket ingress for each cell through the signed
  peer tunnel, but the test coverage for close codes and reconnections
  across nodes is thinner than for one node. If latency is important,
  send the traffic of a cell to its owner node.
- An outbound Durable Object WebSocket keeps its cell resident, and the
  connection does not continue when the cell moves to a different node:
  keep the connection intent in storage, and connect again after the
  activation. (A Worker's outbound socket stops with its request, the
  same as on Cloudflare.) A node limits how much residency the outbound
  sockets can hold.
- Windows is not available, and Intel Macs get no prebuilt binaries; a
  build from source works.
- celld has no central placement controller, so it does not rebalance the
  fleet when a node joins. Normal traffic places an unowned or released cell
  on a node with capacity.
- Updates are manual: the installer keeps immutable releases behind one
  `current` pointer, so a previous SHA is the rollback. There is no
  automatic update agent.
