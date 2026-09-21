# Testing

celld makes three promises:

- An acknowledged write is durable.
- A cell has one writer at a time.
- Code written for Cloudflare Workers and Durable Objects operates the
  same on celld.

We try to break each promise at the layer where a failure shows most
clearly. We test the API contract with differential execution against the
Cloudflare runtime. We check the coordination protocol against an
exhaustively model-checked specification, and we test it with
deterministic simulation. We test the full system with fault injection
on live fleets.

## Conformance: two runtimes, one output

We run the same Workers and Durable Objects program on both workerd,
Cloudflare's production runtime, and celld. The two outputs must match,
so each test checks celld against the Cloudflare runtime.
The reference output comes from workerd, so the expected behavior does
not depend on the celld implementation.

We expand the corpus with fixtures for each new API, and each fixture must
give the same output on both engines. We also port test suites from workerd:
the Durable Objects contract, the web-platform globals, and the upstream
Web Platform Tests. Before a release, we replay storage, SQL, alarm,
stream, WebSocket, and lifecycle scenarios through the full `celld` binary
in each deployment mode.

## Specification: exhaustive at small size

The coordination protocol is also specified in TLA+. Heyang Zhou wrote
the specifications against celld v0.1.0, and his model checking found
four bugs and a split-brain that lost an acknowledged write. All are
fixed. None had surfaced in our own review or testing.

Where simulation samples schedules, the checker enumerates them: at a
small configuration it visits every reachable state. The model grants
the implementation a linearizable object store and perfect shared
clocks, so a violation it finds needs no clock skew and no storage
anomaly to occur. The invariants are the first two promises at the top
of this page: one writer for each epoch, and no acknowledged write
lost. The checker also verifies that the epoch in the key prevents a stale
owner's late writes from causing the loss of an acknowledged write.

Every configuration carries a pinned expected verdict, and most of the
verdicts are failures: each failing configuration models a bug the
protocol once had, or a deliberately broken checker, and the model
must produce the counterexample. If a configuration stops producing its
expected failure, we investigate and repair the check.

Some specifications model a proposed protocol before it is built. When
the fence on write acknowledgments was redesigned, the design-stage
check produced an eight-state counterexample against the version it
replaced: a dormant cell resumes at its old epoch while its release is
in flight, acknowledges a write, and the takeover that follows
restores without it. The fix shipped; the counterexample stays as a
pinned failure.

The checker has also removed code: celld once sealed a cell's durable
history at restore, and the verdicts showed the seal defended only the
return of an unacknowledged write, which celld does not promise to prevent.
The seal could also turn a recoverable ordering error into permanent data
loss, so celld no longer uses it.

We update the specifications manually alongside the protocol changes.
They do not run in continuous integration, because an outdated model can
give false confidence. A separate record tracks what the model does not
yet describe, including guarantees weaker than those in the code.
Simulation checks each commit, and model checking adds exhaustive coverage
of small configurations.

## Simulation: the protocol under adversarial schedules

The dangerous bugs live in the coordination: a crash during an ownership
handoff, a lease renewal that races a takeover, an alarm that fires
against a partially restored cell. These windows are nanoseconds wide and
open rarely, so a test cannot wait for them.

Thus the coordination protocol is a
[pure decision core](https://github.com/denoland/celld/tree/main/crates/logic)
with no I/O of its own: the clock, the randomness, and the object store
are interfaces, and a simulator drives the core. The simulated store
injects latency, compare-and-swap races, and lost responses; the clocks
drift apart; a node can crash at each await point. Scripted adversaries
play the cells: a handler that never returns, a write stream that stops
halfway. V8 stays out of the simulation, because V8 is not deterministic.

A seeded scheduler drives each run, so we can reproduce a failure exactly
and keep its seed until we fix the bug. We check safety properties for
two writers in one epoch, a lost acknowledged write, and an expired lease
that returns. We also check liveness: each armed alarm fires, and ownership
settles on one node after a crash. A property must survive tens of thousands
of seeds, and the core protocols have run through millions of different
schedules.

We also run deliberately broken variants of the protocol to verify that
the checkers detect the faults. A checker that accepts a broken protocol
cannot protect the corresponding property.

## Live fleets: what simulation cannot see

Simulation cannot see the real S3 tail latency, the real kernel and
filesystem behavior, or V8 under memory pressure. The third layer is
therefore a permanent fleet lab: standard VMs from standard providers,
and a real bucket. The workloads rotate: chat rooms under many WebSocket
connections, working sets that shift across tens of thousands of cells
(each cell has a unique checksum), deployment cutovers under load, and
runs that fill the nodes to the memory limit. The lab qualifies each
release and tests greater density and more faults between releases. We
archive each run's configuration, verification sweeps, node journals,
kernel logs, and phase timings. We preserve failed runs as well as
successful ones, because both provide evidence of the system's behavior.

We inject the faults between verification passes. A pass fetches each
cell through different nodes and compares the durable state exactly: the
status, the body, and the full message ledger. Each run therefore records
the state before and after the fault. A cell can be unavailable for a short
time while its ownership moves, but its committed state must
stay complete, and a live node must serve that state again.

The scenarios attack every seam that we know:

- Stop a node with `SIGKILL` in the middle of a write stream and delete
  its local database, so recovery can only come from the bucket. Every
  acknowledged write comes back, because the output gate held each
  response until the write was durable.
- Freeze an owner node, write to its cells through other nodes, and
  unfreeze it. The node sees that its lease moved and refuses to serve
  the old state, and each write from the other nodes lands exactly one
  time: one epoch has at most one writer.
- Cut a node off from the bucket. It fences itself, because a node that
  cannot replicate must not own cells.
- Throttle the bucket, so it answers each request with a 429. The engine
  slows to the write rate of the store and does not amplify the
  throttle, because a node that knows its replicated position does not
  ask a slow store for extra listings.
- Stop a full host at the provider level in the middle of a workload.
  Its cells move to the other nodes, and the returning host joins again
  with no duplicate residency.

Across every run of every scenario, no acknowledged write was lost and
no committed state was damaged; the verification sweeps show zero body
faults, zero status faults, and zero lost messages.

## A few numbers we trust

Each number includes its measurement conditions, so you can assess whether
it applies to your workload.

- **The epoch fence holds under contention.** Five hundred claimants
  tried at the same time to own the same cells: 5,500 attempts, one
  writer for each epoch, zero violations.
- **A warm resident request is local.** A request to a resident cell does
  zero bucket operations and returns in p50 ~1.1 ms and p99 ~7 ms (a
  fixed-host measurement). Only a cold activation touches object storage.
- **A durable write waits for a durability proof.** A single node proves
  each write through the bucket, so one storage round trip is the minimum
  latency for that write. A fleet of two or more nodes can prove a write
  when each follower holds it on disk, and a follower fsync is much
  faster than a storage round trip: a lab fleet measured about 600 ms for
  a bucket proof and about 25 ms for a fleet proof. The bucket upload
  races every fleet proof and either one proves the write, so a slow
  follower cannot make a write slower than a bucket proof. Concurrent
  writes to one cell join one shared upload, so the throughput of one
  cell is not one round trip for each write.
- **A restore is normal work.** Placement handles the restore of an
  inactive cell as ordinary work, and not as an emergency procedure. The
  measured restore times come from the retired external replicator, so this
  page gives no number until a fleet run measures `celld-ltx`.
- **Ten small nodes held real scale.** A fleet of ten nodes, each with 4
  vCPU and 8 GB, held 10,000 resident cells and 20,000 concurrent
  WebSocket connections. We stopped two of the ten nodes, and the data of
  every cell was available again on another node in ~11 s at the tail
  (with reserve headroom).

## The failure edges we intend to find

A fleet at its resident limit has no space for the cells of a lost node,
so the loss of multiple nodes degrades the service. We test recovery with
and without reserve capacity to measure this limit.

Report a schedule that breaks a guarantee, a fault that we do not test,
or a measurement that you cannot reproduce in the
[issue tracker](https://github.com/denoland/celld/issues).
