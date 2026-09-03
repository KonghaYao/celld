// node:http / node:https for celld (OpenNext / Next.js on Workers).
//
// Wrangler bundles leave `http` external; without a real IncomingMessage,
// OpenNext's NodeNextRequest cookie parsing sees non-string `headers.cookie`
// when `http.IncomingMessage` was the pass-through __nodeStub.
//
// Injected lazily like node:stream / node:events.
(() => {
  if (globalThis.__httpModule && globalThis.__httpsModule) return;

  const EE = process.getBuiltinModule("node:events");
  const { EventEmitter } = EE;
  const stream = process.getBuiltinModule("node:stream");
  const { Readable, Writable } = stream;

  class IncomingMessage extends Readable {
    constructor(socketOrOpts) {
      super({ autoDestroy: true });
      this.httpVersion = "1.1";
      this.httpVersionMajor = 1;
      this.httpVersionMinor = 1;
      this.headers = {};
      this.rawHeaders = [];
      this.trailers = {};
      this.rawTrailers = [];
      this.complete = false;
      this.aborted = false;
      this.upgrade = null;
      this.url = "";
      this.method = "GET";
      this.statusCode = null;
      this.statusMessage = null;
      this.socket =
        socketOrOpts && typeof socketOrOpts === "object" ? socketOrOpts : null;
      this.connection = this.socket;
      this.setTimeout = () => this;
    }

    _read() {}
    destroy(error) {
      this.aborted = true;
      if (error) this.emit("error", error);
      this.emit("close");
      if (typeof super.destroy === "function") super.destroy(error);
    }
  }

  class ServerResponse extends Writable {
    constructor(req) {
      super({ autoDestroy: true });
      this.statusCode = 200;
      this.statusMessage = "OK";
      this.headers = {};
      this.headersSent = false;
      this.finished = false;
      this.writableEnded = false;
      this.req = req;
      this.socket = req?.socket ?? null;
      this.connection = this.socket;
      this.setTimeout = () => this;
    }

    setHeader(name, value) {
      if (this.headersSent) return;
      const key = String(name).toLowerCase();
      const val = Array.isArray(value) ? value.join(", ") : String(value);
      this.headers[key] = val;
    }

    getHeader(name) {
      return this.headers[String(name).toLowerCase()];
    }

    getHeaders() {
      return { ...this.headers };
    }

    hasHeader(name) {
      return this.headers[String(name).toLowerCase()] !== undefined;
    }

    removeHeader(name) {
      delete this.headers[String(name).toLowerCase()];
    }

    writeHead(statusCode, statusMessage, headers) {
      let msg = statusMessage;
      let hdrs = headers;
      if (typeof statusMessage === "object" && statusMessage !== null) {
        hdrs = statusMessage;
        msg = undefined;
      }
      this.statusCode = statusCode;
      if (typeof msg === "string") this.statusMessage = msg;
      if (hdrs && typeof hdrs === "object") {
        for (const [k, v] of Object.entries(hdrs)) this.setHeader(k, v);
      }
      this.headersSent = true;
      return this;
    }

    write(chunk, encoding, cb) {
      if (typeof encoding === "function") {
        cb = encoding;
        encoding = undefined;
      }
      this.emit("data", chunk);
      if (typeof super.write === "function" && chunk !== undefined) {
        return super.write(chunk, encoding, cb);
      }
      if (typeof cb === "function") cb();
      return true;
    }

    end(data, encoding, cb) {
      if (typeof data === "function") {
        cb = data;
        data = undefined;
      }
      if (data !== undefined) this.write(data, encoding);
      this.finished = true;
      this.writableEnded = true;
      this.headersSent = true;
      this.emit("end");
      this.emit("finish");
      if (typeof super.end === "function") {
        return super.end(undefined, encoding, cb);
      }
      if (typeof cb === "function") cb();
      return this;
    }
  }

  class Agent extends EventEmitter {
    constructor(options = {}) {
      super();
      this.options = options;
      this.maxSockets = options.maxSockets ?? Infinity;
      this.maxFreeSockets = options.maxFreeSockets ?? 256;
      this.sockets = {};
      this.freeSockets = {};
      this.requests = {};
    }

    destroy() {
      this.emit("destroy");
    }
  }

  const notImplemented = (name) => () => {
    throw new Error(`node:http ${name} is not implemented in celld`);
  };

  const http = {
    IncomingMessage,
    ServerResponse,
    Agent,
    METHODS: [
      "ACL", "BIND", "CHECKOUT", "CONNECT", "COPY", "DELETE", "GET", "HEAD",
      "LINK", "LOCK", "M-SEARCH", "MERGE", "MKACTIVITY", "MKCALENDAR",
      "MKCOL", "MOVE", "NOTIFY", "OPTIONS", "PATCH", "POST", "PROPFIND",
      "PROPPATCH", "PURGE", "PUT", "REBIND", "REPORT", "SEARCH", "SOURCE",
      "SUBSCRIBE", "TRACE", "UNBIND", "UNLINK", "UNLOCK", "UNSUBSCRIBE",
    ],
    STATUS_CODES: {
      100: "Continue", 200: "OK", 201: "Created", 204: "No Content",
      301: "Moved Permanently", 302: "Found", 304: "Not Modified",
      400: "Bad Request", 401: "Unauthorized", 403: "Forbidden",
      404: "Not Found", 500: "Internal Server Error", 502: "Bad Gateway",
      503: "Service Unavailable",
    },
    createServer: notImplemented("createServer"),
    request: notImplemented("request"),
    get: notImplemented("get"),
    validateHeaderName: notImplemented("validateHeaderName"),
    validateHeaderValue: notImplemented("validateHeaderValue"),
    maxHeaderSize: 16384,
    globalAgent: new Agent(),
  };

  globalThis.__httpModule = http;
  globalThis.__httpsModule = {
    ...http,
    Agent,
    globalAgent: new Agent(),
  };
})();
