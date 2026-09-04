// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Lazy Node `url` compatibility used by bundled Node applications. The Web
// URL classes already come from the isolate prelude; this module adds the
// legacy parse/format API and file URL conversion helpers that Next.js emits.
(function () {
  if (globalThis.__urlModule) return;

  const WebURL = globalThis.URL;
  const WebURLSearchParams = globalThis.URLSearchParams;

  function invalidArgType(name, expected, value) {
    const actual = value === null ? "null" : typeof value;
    return new TypeError(
      `The "${name}" argument must be of type ${expected}. Received ${actual}`,
    );
  }

  function decodeQueryComponent(value) {
    try {
      return decodeURIComponent(value.replace(/\+/g, " "));
    } catch {
      return value;
    }
  }

  function parseQuery(search) {
    const query = Object.create(null);
    if (!search) return query;
    for (const field of search.split("&")) {
      if (!field) continue;
      const separator = field.indexOf("=");
      const key = decodeQueryComponent(
        separator === -1 ? field : field.slice(0, separator),
      );
      const value = decodeQueryComponent(
        separator === -1 ? "" : field.slice(separator + 1),
      );
      if (query[key] === undefined) query[key] = value;
      else if (Array.isArray(query[key])) query[key].push(value);
      else query[key] = [query[key], value];
    }
    return query;
  }

  function encodeQueryComponent(value) {
    return encodeURIComponent(String(value));
  }

  function formatQuery(query) {
    if (typeof query === "string") return query;
    if (!query || typeof query !== "object") return "";
    const fields = [];
    for (const key of Object.keys(query)) {
      const values = Array.isArray(query[key]) ? query[key] : [query[key]];
      for (const value of values) {
        fields.push(
          `${encodeQueryComponent(key)}=${encodeQueryComponent(value ?? "")}`,
        );
      }
    }
    return fields.join("&");
  }

  function Url() {
    this.protocol = null;
    this.slashes = null;
    this.auth = null;
    this.host = null;
    this.port = null;
    this.hostname = null;
    this.hash = null;
    this.search = null;
    this.query = null;
    this.pathname = null;
    this.path = null;
    this.href = "";
  }

  function applyWebURL(target, parsed, parseQueryString, protocol) {
    target.protocol = protocol === undefined ? parsed.protocol : protocol;
    target.slashes = true;
    if (parsed.username || parsed.password) {
      target.auth = decodeURIComponent(
        parsed.username + (parsed.password ? `:${parsed.password}` : ""),
      );
    }
    target.host = parsed.host;
    target.port = parsed.port || null;
    target.hostname = parsed.hostname;
    target.hash = parsed.hash || null;
    target.search = parsed.search || null;
    const rawQuery = parsed.search ? parsed.search.slice(1) : "";
    target.query = parseQueryString ? parseQuery(rawQuery) : rawQuery || null;
    target.pathname = parsed.pathname || null;
    target.path = (parsed.pathname || "") + (parsed.search || "") || null;
    target.href = parsed.href;
    return target;
  }

  function parse(value, parseQueryString = false, slashesDenoteHost = false) {
    if (typeof value !== "string") {
      throw invalidArgType("url", "string", value);
    }

    const result = new Url();
    const source = value.trim();
    const scheme = /^([A-Za-z][A-Za-z\d+.-]*:)/.exec(source);
    if (scheme) {
      try {
        return applyWebURL(
          result,
          new WebURL(source),
          parseQueryString,
          scheme[1].toLowerCase(),
        );
      } catch {
        // Legacy url.parse accepts more than WHATWG URL. Preserve its useful
        // path fields below rather than replacing them with a callable stub.
        result.protocol = scheme[1].toLowerCase();
      }
    } else if (slashesDenoteHost && source.startsWith("//")) {
      try {
        return applyWebURL(
          result,
          new WebURL(`http:${source}`),
          parseQueryString,
          null,
        );
      } catch {
        // Fall through to the path-only legacy shape.
      }
    }

    let rest = scheme ? source.slice(scheme[1].length) : source;
    const hashAt = rest.indexOf("#");
    if (hashAt !== -1) {
      result.hash = rest.slice(hashAt);
      rest = rest.slice(0, hashAt);
    }
    const queryAt = rest.indexOf("?");
    let rawQuery = "";
    if (queryAt !== -1) {
      result.search = rest.slice(queryAt);
      rawQuery = rest.slice(queryAt + 1);
      rest = rest.slice(0, queryAt);
    }
    result.query = parseQueryString ? parseQuery(rawQuery) : rawQuery || null;
    result.pathname = rest || null;
    result.path = rest + (result.search || "") || null;
    result.href = source;
    return result;
  }

  function format(urlObject) {
    if (typeof urlObject === "string") urlObject = parse(urlObject);
    if (!urlObject || typeof urlObject !== "object") {
      throw invalidArgType("urlObject", "object or string", urlObject);
    }
    if (urlObject instanceof WebURL) return urlObject.href;

    let protocol = urlObject.protocol || "";
    if (protocol && !protocol.endsWith(":")) protocol += ":";
    let host = urlObject.host;
    if (host == null && urlObject.hostname != null) {
      host = String(urlObject.hostname);
      if (host.includes(":") && !host.startsWith("[")) host = `[${host}]`;
      if (urlObject.port) host += `:${urlObject.port}`;
    }

    const hasHost = host != null && host !== "";
    const slashes = Boolean(urlObject.slashes || hasHost);
    let output = protocol;
    if (slashes) output += "//";
    if (urlObject.auth) {
      const auth = String(urlObject.auth).split(":").map(encodeURIComponent).join(":");
      output += `${auth}@`;
    }
    if (hasHost) output += host;

    let pathname = urlObject.pathname == null ? "" : String(urlObject.pathname);
    if (hasHost && pathname && !pathname.startsWith("/")) pathname = `/${pathname}`;
    output += pathname;

    let search = urlObject.search;
    if (search == null || search === "") {
      const query = formatQuery(urlObject.query);
      search = query ? `?${query}` : "";
    } else {
      search = String(search);
      if (!search.startsWith("?")) search = `?${search}`;
    }
    output += search;

    if (urlObject.hash) {
      const hash = String(urlObject.hash);
      output += hash.startsWith("#") ? hash : `#${hash}`;
    }
    return output;
  }

  function currentDirectory() {
    try {
      if (typeof globalThis.process?.cwd === "function") {
        return String(globalThis.process.cwd());
      }
    } catch {
      // Workers do not promise a process working directory.
    }
    return "/";
  }

  function absolutePath(path) {
    const windowsDrive = /^[A-Za-z]:[\\/]/.test(path);
    if (windowsDrive) return `/${path.replace(/\\/g, "/")}`;
    if (path.startsWith("/")) return path;
    const parts = `${currentDirectory().replace(/\/$/, "")}/${path}`.split("/");
    const normalized = [];
    for (const part of parts) {
      if (!part || part === ".") continue;
      if (part === "..") normalized.pop();
      else normalized.push(part);
    }
    return `/${normalized.join("/")}${path.endsWith("/") ? "/" : ""}`;
  }

  function pathToFileURL(path) {
    if (typeof path !== "string") {
      throw invalidArgType("path", "string", path);
    }
    const absolute = absolutePath(path);
    const encoded = encodeURI(absolute)
      .replace(/\?/g, "%3F")
      .replace(/#/g, "%23");
    return new WebURL(`file://${encoded}`);
  }

  function fileURLToPath(value) {
    const parsed = value instanceof WebURL ? value : new WebURL(value);
    if (parsed.protocol !== "file:") {
      throw new TypeError("The URL must be of scheme file");
    }
    if (parsed.hostname && parsed.hostname !== "localhost") {
      throw new TypeError("File URL host must be empty or localhost");
    }
    if (/%2f/i.test(parsed.pathname)) {
      throw new TypeError("File URL path must not include encoded / characters");
    }
    let path = decodeURIComponent(parsed.pathname);
    if (globalThis.process?.platform === "win32" && /^\/[A-Za-z]:/.test(path)) {
      path = path.slice(1).replace(/\//g, "\\");
    }
    return path;
  }

  globalThis.__urlModule = {
    Url,
    URL: WebURL,
    URLSearchParams: WebURLSearchParams,
    parse,
    format,
    pathToFileURL,
    fileURLToPath,
  };
})();
