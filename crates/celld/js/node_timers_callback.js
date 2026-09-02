// node:timers (callback API) for Cells.
//
// Nitro/h3 `send()` schedules `res.end` on `setImmediate`. The generic
// pass-through builtin stub is callable but never invokes the callback,
// so `localFetch` hangs. These wrap the host timer ops.
(() => {
  const setImmediate = (callback, ...args) => {
    if (typeof callback !== "function") {
      const e = new TypeError(
        'The "callback" argument must be of type function');
      e.code = "ERR_INVALID_ARG_TYPE";
      throw e;
    }
    return globalThis.setTimeout(() => callback(...args), 0);
  };
  const clearImmediate = (handle) => globalThis.clearTimeout(handle);
  globalThis.__timersModule = {
    setTimeout: (...a) => globalThis.setTimeout(...a),
    clearTimeout: (...a) => globalThis.clearTimeout(...a),
    setInterval: (...a) => globalThis.setInterval(...a),
    clearInterval: (...a) => globalThis.clearInterval(...a),
    setImmediate,
    clearImmediate,
  };
})();
