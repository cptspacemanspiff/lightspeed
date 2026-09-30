// JavaScript evaluated in the sandbox before the script. It takes the host
// functions off the global object, replaces every source of nondeterminism
// with one the host controls, and builds the `tools` namespace.
//
// The host injects: __hostCall(name, argsJson) -> Promise<string>,
// __hostLog(level, text), __hostClock() -> number, __hostDone(ok, json),
// __toolNames (JSON), __inputJson, __seed0, __seed1.

export const PRELUDE = String.raw`
(function () {
  "use strict";
  const hostCall = globalThis.__hostCall;
  const hostLog = globalThis.__hostLog;
  const hostClock = globalThis.__hostClock;
  const hostDone = globalThis.__hostDone;
  const toolNames = JSON.parse(globalThis.__toolNames);
  const inputJson = globalThis.__inputJson;
  let a = globalThis.__seed0 | 0;
  let b = globalThis.__seed1 | 0;
  let c = 0x9e3779b9 | 0;
  let d = 1;
  for (const name of [
    "__hostCall", "__hostLog", "__hostClock", "__hostDone",
    "__toolNames", "__inputJson", "__seed0", "__seed1",
  ]) {
    delete globalThis[name];
  }

  // Engines differ in where a global lives (the global object or a
  // prototype of it), so clear every holder before shadowing it.
  function unset(name) {
    for (let scope = globalThis; scope; scope = Object.getPrototypeOf(scope)) {
      if (Object.prototype.hasOwnProperty.call(scope, name)) {
        try { delete scope[name]; } catch (_) {}
      }
    }
  }
  function replace(name, value) {
    unset(name);
    Object.defineProperty(globalThis, name, { value, enumerable: true });
  }
  function remove(name) {
    unset(name);
    if (name in globalThis) Object.defineProperty(globalThis, name, { value: undefined });
  }

  // Randomness: sfc32, seeded by the workflow.
  function next32() {
    const t = (((a + b) | 0) + d) | 0;
    d = (d + 1) | 0;
    a = b ^ (b >>> 9);
    b = (c + (c << 3)) | 0;
    c = (c << 21) | (c >>> 11);
    c = (c + t) | 0;
    return t >>> 0;
  }
  for (let i = 0; i < 12; i++) next32();
  Math.random = () => next32() / 4294967296;

  function getRandomValues(array) {
    if (!ArrayBuffer.isView(array) || array instanceof Float32Array ||
        array instanceof Float64Array || array instanceof DataView) {
      throw new TypeError("getRandomValues expects an integer typed array");
    }
    const bytes = new Uint8Array(array.buffer, array.byteOffset, array.byteLength);
    for (let i = 0; i < bytes.length; i++) bytes[i] = next32() & 0xff;
    return array;
  }
  function randomUUID() {
    const bytes = getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = "";
    for (let i = 0; i < 16; i++) hex += (bytes[i] + 0x100).toString(16).slice(1);
    return hex.slice(0, 8) + "-" + hex.slice(8, 12) + "-" + hex.slice(12, 16) +
      "-" + hex.slice(16, 20) + "-" + hex.slice(20);
  }
  replace("crypto", Object.freeze({ getRandomValues, randomUUID }));

  // Time: a logical clock that advances only when a result is delivered.
  const RealDate = Date;
  const startClock = hostClock();
  const SandboxDate = new Proxy(RealDate, {
    construct(target, args, newTarget) {
      return Reflect.construct(target, args.length === 0 ? [hostClock()] : args, newTarget);
    },
    apply() {
      return new RealDate(hostClock()).toString();
    },
  });
  RealDate.now = () => hostClock();
  RealDate.prototype.constructor = SandboxDate;
  globalThis.Date = SandboxDate;
  replace("performance", Object.freeze({ now: () => hostClock() - startClock }));

  // Garbage collection timing, timers, and I/O must not be observable.
  // Not every engine has all of these.
  for (const name of [
    "WeakRef", "FinalizationRegistry",
    "setTimeout", "setInterval", "setImmediate",
    "clearTimeout", "clearInterval", "clearImmediate", "scheduler",
    "fetch", "WebSocket", "EventSource", "caches", "navigator",
  ]) {
    remove(name);
  }

  function format(value) {
    if (typeof value === "string") return value;
    if (value instanceof Error) return value.stack ? String(value.stack) : String(value);
    try {
      const json = JSON.stringify(value);
      return json === undefined ? String(value) : json;
    } catch (_) {
      return String(value);
    }
  }
  const logger = (level) => (...args) => hostLog(level, args.map(format).join(" "));
  replace("console", Object.freeze({
    debug: logger("debug"),
    log: logger("info"),
    info: logger("info"),
    warn: logger("warn"),
    error: logger("error"),
  }));

  class ToolError extends Error {
    constructor(tool, error) {
      super(tool + ": " + error.message);
      this.name = "ToolError";
      this.tool = tool;
      this.code = error.code;
      this.details = error.details;
    }
  }
  replace("ToolError", ToolError);

  async function call(name, args) {
    const json = JSON.stringify(args === undefined ? {} : args);
    if (typeof json !== "string") {
      throw new TypeError(name + ": arguments are not JSON-serializable");
    }
    const reply = JSON.parse(await hostCall(name, json));
    if (!reply.ok) throw new ToolError(name, reply.error);
    return reply.value;
  }

  const tools = {};
  for (const name of toolNames) {
    const parts = name.split(".");
    let node = tools;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!Object.prototype.hasOwnProperty.call(node, parts[i])) node[parts[i]] = {};
      node = node[parts[i]];
    }
    node[parts[parts.length - 1]] = (args) => call(name, args);
  }
  (function freeze(node) {
    for (const key of Object.keys(node)) {
      if (typeof node[key] === "object") freeze(node[key]);
    }
    Object.freeze(node);
  })(tools);
  replace("tools", tools);
  replace("sleep", async (ms) => { await call("sleep", { ms }); });

  function describeError(error) {
    if (error instanceof Error) {
      return { name: String(error.name), message: String(error.message), stack: String(error.stack || "") };
    }
    return { name: "Error", message: "script threw " + format(error), stack: "" };
  }

  globalThis.__start = function (main) {
    delete globalThis.__start;
    Promise.resolve()
      .then(() => main(JSON.parse(inputJson)))
      .then(
        (value) => {
          let json;
          try {
            json = JSON.stringify(value === undefined ? null : value);
          } catch (error) {
            hostDone(false, JSON.stringify({
              name: "TypeError",
              message: "the script's result is not JSON-serializable: " + describeError(error).message,
              stack: "",
            }));
            return;
          }
          hostDone(true, typeof json === "string" ? json : "null");
        },
        (error) => hostDone(false, JSON.stringify(describeError(error))),
      );
  };
})();
`;
