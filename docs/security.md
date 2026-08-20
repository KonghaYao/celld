# Security

celld is an alpha. It is not safe for hostile multi-tenant use. Security fixes
apply to the latest release only, so older alpha builds do not receive fixes.

## Separate the public and internal listeners

celld opens two HTTP listeners. The public listener serves the deployed Worker,
and the internal listener serves the peer protocol and the operator API.

Use `--listen` for the public listener. Expose only this listener through a load
balancer, a reverse proxy, or a public firewall rule.

Use `--internal-listen` for the internal listener. Its default address is
`127.0.0.1:0`, so celld selects an available loopback port at each start.
The startup output reports the selected address.

Use `--advertise` to give peers the address of the internal listener. Bind the
internal listener to a private interface, or protect it with a private overlay.
Do not expose this listener to the public internet.

An explicit advertised address requires an explicit internal-listener address.
Set both command-line options, or use their equivalent environment variables.
celld also rejects an explicit non-loopback public listener without an explicit
internal listener. This rule identifies an obsolete one-listener configuration.

celld cannot verify that an advertised hostname or a translated port reaches
the internal listener. You must route the advertised address to the internal
listener, and you must not route it to the public Worker listener.

The public listener reserves only `/__celld/health`. A healthy node returns a
200 response with `{"ok":true}`, and an unhealthy node returns a 503 response.
The deployed Worker owns `/health` and every other public path.

The internal listener does not pass an unknown path to the Worker. It returns a
404 response, so an operator request cannot become an application request.

## Set the forwarded-header policy

A Worker reads the request URL from `request.url`. An application can route on
the hostname and build an absolute link from this URL. Therefore, celld ignores
`X-Forwarded-Host` and `X-Forwarded-Proto` by default.

Set `--trust-forwarded-headers` only when a trusted proxy replaces both headers.
celld then reads the last value in each header, so an earlier client value does
not override the proxy value. The equivalent environment variable is
`CELLD_TRUST_FORWARDED_HEADERS=1`.

celld always takes the path and query from the request target. It ignores an
absolute-form request target's scheme and authority, so a direct client cannot
bypass the host policy through the request line.

## Protect the internal listener

Most of the operator API does not authenticate its requests. A client that
can reach the internal listener can inspect state, start direct work, evict a
cell, or stop the process. Therefore, a firewall or a private overlay must
restrict access to trusted operators and fleet nodes.

The D1 route is the exception, because a D1 database holds application data
and runs the SQL that a caller sends. That route authenticates each request
with the fleet secret, and the unauthenticated `/do/NAME` route refuses a D1
database. An unauthenticated client on the internal listener can therefore
stop a node, but it cannot read or change the contents of a D1 database.
The refusal protects only the D1 database: the `/do/NAME` route still sends
an unauthenticated request to the `fetch` handler of an ordinary Durable
Object, so that handler runs whatever code it contains for that request. The
difference is the SQL: only the D1 route runs arbitrary SQL that the caller
wrote, and a Durable Object runs only its own code.

Peer requests on the same listener keep their protocol authentication. Each
peer request has an HMAC, a body signature, a clock limit, and replay
protection. The private network adds protection, but it does not replace the
peer authentication.

celld does not terminate TLS on the internal listener. Use an encrypted overlay
such as WireGuard or Tailscale when the private network does not provide the
required confidentiality.

## Internal operator API

The internal operator API is available in the released binary. The API is an
alpha interface, so a release can change its paths or response formats.

- `/state` reports the current occupancy, eviction, and restoration values. It
  remains available while a graceful shutdown drains existing work.
- `/cell/NAME` resolves or activates a cell for an operator check.
- `/evict/NAME` evicts a resident cell.
- `/do/NAME` sends a direct Durable Object request. This route refuses a D1
  database, because the route does not authenticate its caller.
- `/__d1/SCOPE` sends SQL to a D1 database. This route authenticates each
  request with the fleet secret, so a caller needs the bucket credentials.
  The `celld d1` command uses this route.
- `POST /shutdown` starts a graceful ownership handoff.
- `POST /shutdown?handoff=preserve` prepares a clean same-node reload and keeps
  the ownership records.
- `/__celld/probe` serves the signed diagnostic probe.

The peer protocol also uses reserved internal paths. An operator must not call
these paths directly, and celld continues to authenticate each peer request.

## Protect the fleet bucket

The fleet bucket is the root of authority for the fleet. It stores the
deployments, the cell state, the ownership leases, the node leases, and the
shared peer-authentication secret.

A person who holds the bucket credentials controls the fleet. Give each
credential access to one fleet bucket only, and replace a credential after a
suspected disclosure.

## Keep one writer for each cell

Each cell is a SQLite database with one writer. One node owns a cell at a time,
and an ownership epoch fences each cell.

A node that loses its lease cannot modify the current cell state. The
[ownership and fencing](fencing.md) page describes this mechanism.

A fleet has no shared multi-tenant scheduler or shared placement layer. A
defective cell can access only its own database, but it can consume resources
on its fleet nodes.

## Protect the public application

celld does not authenticate the users of the deployed application. It also does
not terminate public TLS. Put the required authentication and TLS in front of
the public listener.

Keep the internal listener private, and keep the bucket credentials secret.
See the [limitations](limitations.md) page for the complete alpha boundary.
