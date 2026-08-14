# Examples

These small Wrangler projects demonstrate progressively more of the Worker and
Durable Object surface supported by `celld`:

- `hello/` — a stateless Worker `fetch` handler
- `webapi/` — common Web Platform APIs
- `counter/` — a SQLite-backed Durable Object
- `vectordb/` — nearest-color search with a per-object `vec0` index
- `async/` — a timer, an outbound fetch, and asynchronous storage
- `body/` — request and response bodies
- `router/` — Worker-to-Durable-Object routing
- `wsecho/` — WebSocket echo with hibernation
- `wsclient/` — outbound WebSocket client from a Durable Object
- `alarm/` — a Durable Object alarm handler
- `rpc/` — JS RPC: Durable Object methods, a named entrypoint, callbacks,
  `RpcTarget`, and promise pipelining
- `wasm/` — a Durable Object counter in Rust, compiled to Wasm with
  [workers-rs](https://github.com/cloudflare/workers-rs); needs a build
  step first (see its [README](wasm/README.md))

Deploy an example from its directory to the same bucket the nodes use:

```sh
celld deploy . --bucket s3://my-cells-bucket
```

They are examples, not the complete compatibility test suite.
