// A cron trigger that records every tick, and a fetch handler that reads the
// record back.
//
// The `scheduled` handler runs in the reserved cron cell's isolate, and a
// `fetch` lands in the stateless Worker pool, so the two never share an
// in-process variable. Writing the tick into a Durable Object is what makes it
// visible to both — and it is the pattern a real cron job wants anyway, since
// only a cell's storage survives the isolate the handler ran in.
//
// Plain Worker code: runs identically on Cloudflare.
export class Ticks {
  constructor(state, env) { this.state = state; }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/tick") {
      const { cron, scheduledTime } = await request.json();
      const count = ((await this.state.storage.get("count")) ?? 0) + 1;
      await this.state.storage.put({ count, lastCron: cron, lastTick: scheduledTime });
      return new Response(null, { status: 204 });
    }
    return new Response(JSON.stringify({
      count: (await this.state.storage.get("count")) ?? 0,
      lastCron: (await this.state.storage.get("lastCron")) ?? null,
      lastTick: (await this.state.storage.get("lastTick")) ?? null,
    }));
  }
}
const log = (env) => env.TICKS.get(env.TICKS.idFromName("log"));
export default {
  async fetch(request, env) {
    return log(env).fetch(request);
  },
  // Both expressions in wrangler.jsonc arrive here, one call each, and
  // `controller.cron` says which one is running. `scheduledTime` is the
  // occurrence itself, not the moment this handler started, so a late run
  // still records the minute it was scheduled for.
  async scheduled(controller, env, ctx) {
    await log(env).fetch("http://cron/tick", {
      method: "POST",
      body: JSON.stringify({
        cron: controller.cron,
        scheduledTime: controller.scheduledTime,
      }),
    });
  },
};
