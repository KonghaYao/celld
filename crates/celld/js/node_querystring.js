// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Lazy Node `querystring` compatibility. OpenNext uses the legacy parse/stringify
// pair to normalize redirect Location query strings.
(function () {
  if (globalThis.__querystringModule) return;

  const stringifyPrimitive = (value) => {
    if (typeof value === "string") return value;
    if (
      typeof value === "number" && Number.isFinite(value) ||
      typeof value === "bigint" ||
      typeof value === "boolean"
    ) {
      return String(value);
    }
    return "";
  };

  const escape = (value) => encodeURIComponent(String(value));

  const unescape = (value) => {
    value = String(value).replace(/\+/g, " ");
    try {
      return decodeURIComponent(value);
    } catch {
      // Node's querystring parser is forgiving of malformed percent escapes.
      return value.replace(/(?:%[0-9A-Fa-f]{2})+/g, (encoded) => {
        try {
          return decodeURIComponent(encoded);
        } catch {
          return encoded;
        }
      });
    }
  };

  function stringify(value, separator = "&", equals = "=", options = undefined) {
    if (value === null || typeof value !== "object") return "";
    separator = String(separator);
    equals = String(equals);
    const encode = typeof options?.encodeURIComponent === "function"
      ? options.encodeURIComponent
      : escape;
    const fields = [];
    for (const key of Object.keys(value)) {
      const encodedKey = encode(String(key));
      const values = Array.isArray(value[key]) ? value[key] : [value[key]];
      for (const item of values) {
        fields.push(`${encodedKey}${equals}${encode(stringifyPrimitive(item))}`);
      }
    }
    return fields.join(separator);
  }

  function parse(value, separator = "&", equals = "=", options = undefined) {
    const result = Object.create(null);
    if (typeof value !== "string" || value.length === 0) return result;
    separator = String(separator);
    equals = String(equals);
    const decode = typeof options?.decodeURIComponent === "function"
      ? options.decodeURIComponent
      : unescape;
    const configuredMax = Number(options?.maxKeys);
    const maxKeys = options?.maxKeys === undefined || !Number.isFinite(configuredMax)
      ? 1000
      : Math.trunc(configuredMax);
    const fields = value.split(separator);
    const limit = maxKeys > 0 ? Math.min(fields.length, maxKeys) : fields.length;
    for (let index = 0; index < limit; index++) {
      const field = fields[index];
      const split = field.indexOf(equals);
      const key = decode(split === -1 ? field : field.slice(0, split));
      const item = decode(split === -1 ? "" : field.slice(split + equals.length));
      if (result[key] === undefined) result[key] = item;
      else if (Array.isArray(result[key])) result[key].push(item);
      else result[key] = [result[key], item];
    }
    return result;
  }

  globalThis.__querystringModule = {
    decode: parse,
    encode: stringify,
    escape,
    parse,
    stringify,
    unescape,
  };
})();
