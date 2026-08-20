// D1: https://developers.cloudflare.com/d1/worker-api/
//
// A guestbook on one D1 database. `env.DB` addresses a cell holding one SQLite
// file with one writer, so when a write resolves it is in the bucket. Scale
// comes from many databases, not one big one: give each tenant its own
// `database_name`.
//
// `batch()`, `withSession()` and `dump()` throw rather than half-work.
const SCHEMA = `
CREATE TABLE IF NOT EXISTS entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  message TEXT NOT NULL,
  at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS entries_at ON entries (at DESC);
`;

// One `exec()` per isolate, not one per request. The statements are
// idempotent, so a racing isolate is harmless. A rejection resets the memo:
// `??=` alone would cache the failed promise, and one transient error would
// then fail every later request in this isolate with the same stale cause.
let ready;
const migrate = (env) =>
  (ready ??= env.DB.exec(SCHEMA).catch((error) => {
    ready = undefined;
    throw error;
  }));

export default {
  async fetch(request, env) {
    await migrate(env);
    const url = new URL(request.url);

    if (request.method === "POST") {
      const { name, message } = await request.json();
      const written = await env.DB
        .prepare("INSERT INTO entries (name, message, at) VALUES (?, ?, ?)")
        .bind(name, message, Date.now())
        .run();
      return Response.json({
        id: written.meta.last_row_id,
        changes: written.meta.changes,
      }, { status: 201 });
    }

    const count = await env.DB
      .prepare("SELECT count(*) AS n FROM entries")
      .first("n");
    if (url.pathname === "/count") return Response.json({ count });

    // `all()` copies the whole result set into this Worker, so bound it:
    // celld refuses a result set above 100,000 rows.
    const { results } = await env.DB
      .prepare("SELECT id, name, message, at FROM entries ORDER BY at DESC LIMIT ?")
      .bind(20)
      .all();
    return Response.json({ count, entries: results });
  },
};
