(function (send, cancelHost, keep, shouldContinueHost, catalogJson, programJson) {
    "use strict";
    // Capture intrinsics before user code can replace globals or prototypes.
    const parse = JSON.parse;
    const stringify = JSON.stringify;
    const keys = Object.keys;
    const prototype = Object.getPrototypeOf;
    const descriptor = Object.getOwnPropertyDescriptor;
    const hasOwn = Object.hasOwn;
    const symbols = Object.getOwnPropertySymbols;
    const create = Object.create;
    const freeze = Object.freeze;
    const define = Object.defineProperty;
    const isArray = Array.isArray;
    const isFinite = Number.isFinite;
    const PlainObject = Object.prototype;
    const NativePromise = Promise;
    const NativeError = Error;
    const NativeTypeError = TypeError;
    const AsyncFunction = (async function () {}).constructor;
    const reflectApply = Reflect.apply;
    const stringSplit = String.prototype.split;
    const arraySlice = Array.prototype.slice;
    const floor = Math.floor;
    const pending = create(null);
    const bindingIds = create(null);

    // Clone only JSON data, rejecting silent JSON.stringify losses such as
    // undefined fields, functions, non-finite numbers, accessors and array holes.
    // Null-prototype copies prevent a user-supplied toJSON hook from running.
    function clone(value, ancestors) {
        if (value === null || typeof value === "string" || typeof value === "boolean") return value;
        if (typeof value === "number" && isFinite(value)) return value;
        if (typeof value !== "object") throw new NativeTypeError("Value is not JSON-compatible");
        if (ancestors.length >= 128) throw new NativeTypeError("JSON nesting is too deep");
        for (let i = 0; i < ancestors.length; i++) {
            if (ancestors[i] === value) throw new NativeTypeError("Cyclic value is not JSON-compatible");
        }
        const array = isArray(value);
        const proto = prototype(value);
        if (!array && proto !== null && proto !== PlainObject) {
            throw new NativeTypeError("Only plain objects and arrays are JSON-compatible");
        }
        if (symbols(value).length !== 0) throw new NativeTypeError("Symbol keys are not JSON-compatible");
        const output = array ? [] : create(null);
        // Also mask inherited toJSON on arrays without affecting JSON elements.
        if (array) define(output, "toJSON", { value: undefined });
        const names = keys(value);
        if (array && names.length !== value.length) {
            throw new NativeTypeError("Sparse arrays and extra array properties are not JSON-compatible");
        }
        ancestors[ancestors.length] = value;
        for (let i = 0; i < names.length; i++) {
            const key = names[i];
            if (array && key !== "" + i) throw new NativeTypeError("Invalid array property");
            const field = descriptor(value, key);
            if (!field || !hasOwn(field, "value")) throw new NativeTypeError("Accessors are not JSON-compatible");
            define(output, key, { value: clone(field.value, ancestors), enumerable: true, configurable: true, writable: true });
        }
        ancestors.length--;
        return output;
    }
    function serialize(value) {
        const ancestors = create(null);
        ancestors.length = 0;
        return stringify(clone(value, ancestors));
    }
    function invoke(id, args) {
        const json = serialize(args);
        let request;
        const promise = new NativePromise(function (resolve, reject) {
            request = send(id, json);
            pending[request] = { resolve, reject };
        });
        return { request, promise };
    }
    // Every call's promise has `cancel()`. A cancel is a request: the call
    // still settles exactly once, with its result if that came first.
    // `await promise.cancel()` waits until it has settled and reports how:
    // `cancelled`, `resolved` (it finished first) or `failed`. The promise
    // itself stays the authority on the outcome.
    async function settledStatus(promise, cancelledKind) {
        try {
            await promise;
            return freeze({ status: "resolved" });
        } catch (error) {
            const kind = error !== null && typeof error === "object" ? error.kind : undefined;
            return freeze({ status: kind === cancelledKind ? "cancelled" : "failed" });
        }
    }
    function withCancel(promise, request) {
        define(promise, "cancel", {
            value: freeze(async function () {
                cancelHost("call", request);
                return await settledStatus(promise, "tool_cancelled");
            }),
        });
        return promise;
    }
    // A call as `env` exposes it: argument errors reject its promise.
    function call(id, args) {
        let issued;
        try {
            issued = invoke(id, args);
        } catch (error) {
            return NativePromise.reject(error);
        }
        return withCancel(issued.promise, issued.request);
    }
    const catalog = parse(catalogJson);
    for (let i = 0; i < catalog.length; i++) {
        bindingIds[catalog[i].name] = catalog[i].binding_id;
    }
    // A program is a module whose default export has a
    // `run(input, env, ctx)` handler. Its capabilities arrive only through
    // `env`, one namespace per contract family, and the host owns every
    // source of nondeterminism: the clock, randomness and timers.
    const program = parse(programJson);
    // A continued execution's clock starts where the previous one stopped.
    let now = hasOwn(program, "clock_ms") ? program.clock_ms : program.started_at_ms;
    function advance(at) {
        if (typeof at === "number" && at > now) now = at;
    }
    function buildEnv() {
        const env = create(null);
        for (let i = 0; i < catalog.length; i++) {
            const binding = catalog[i];
            const id = binding.binding_id;
            if (binding.name === "time.sleep") {
                sleepId = id;
                place(env, binding.name, freeze(function (args) {
                    return sleep(args);
                }));
                continue;
            }
            if (binding.name === "input.open") {
                place(env, binding.name, freeze(function (spec) {
                    let issued;
                    try {
                        issued = invoke(id, spec);
                    } catch (error) {
                        return NativePromise.reject(error);
                    }
                    const opened = issued.promise.then(function (opened) {
                        return inputHandle(opened.executionId, opened.promise);
                    });
                    return withCancel(opened, issued.request);
                }));
                continue;
            }
            if (binding.name === "input.handle") {
                // Take back a request an earlier run detached: the handle
                // is this run's again, withdrawn at its exit unless detached.
                place(env, binding.name, freeze(function (requestId) {
                    let issued;
                    try {
                        issued = invoke(id, { id: requestId });
                    } catch (error) {
                        return NativePromise.reject(error);
                    }
                    const taken = issued.promise.then(function (taken) {
                        return inputHandle(taken.executionId, taken.promise);
                    });
                    return withCancel(taken, issued.request);
                }));
                continue;
            }
            if (binding.name === "agent.run") {
                place(env, binding.name, freeze(function (args) {
                    let issued;
                    try {
                        issued = invoke(id, args);
                    } catch (error) {
                        return NativePromise.reject(error);
                    }
                    const ran = issued.promise.then(undefined, function (error) {
                        throw agentCallFailure(error);
                    });
                    return withCancel(ran, issued.request);
                }));
                continue;
            }
            if (binding.name === "agent.spawn") {
                place(env, binding.name, freeze(function (args) {
                    let issued;
                    try {
                        issued = invoke(id, args);
                    } catch (error) {
                        return NativePromise.reject(error);
                    }
                    const spawned = issued.promise.then(function (spawned) {
                        return agentHandle(spawned.executionId, spawned.promise);
                    });
                    return withCancel(spawned, issued.request);
                }));
                continue;
            }
            place(env, binding.name, freeze(function (args) {
                return call(id, args);
            }));
        }
        return deepFreeze(env);
    }
    function place(root, name, leaf) {
        const segments = reflectApply(stringSplit, name, ["."]);
        let node = root;
        for (let i = 0; i < segments.length - 1; i++) {
            const segment = segments[i];
            if (!hasOwn(node, segment)) define(node, segment, { value: create(null), enumerable: true });
            node = node[segment];
            if (typeof node !== "object") throw new NativeError("Contract " + name + " conflicts with another contract");
        }
        const last = segments[segments.length - 1];
        if (hasOwn(node, last)) throw new NativeError("Contract " + name + " conflicts with another contract");
        define(node, last, { value: leaf, enumerable: true });
    }
    function deepFreeze(node) {
        const names = keys(node);
        for (let i = 0; i < names.length; i++) {
            const value = node[names[i]];
            if (typeof value === "object") deepFreeze(value);
        }
        return freeze(node);
    }

    // `time.sleep` is one durable call that completes once its delay has
    // elapsed; the clock is clamped to its deadline.
    let sleepId = null;
    function sleep(args) {
        if (sleepId === null) {
            return NativePromise.reject(new NativeError("Waiting requires the time.sleep contract"));
        }
        let issued;
        try {
            issued = invoke(sleepId, args);
        } catch (error) {
            return NativePromise.reject(error);
        }
        const slept = issued.promise.then(function (slept) {
            advance(slept.fire_at_ms);
            return slept;
        });
        return withCancel(slept, issued.request);
    }

    // An open input request. Its operations are ordinary calls: `answer`
    // awaits the request's promise, `cancel` cancels it, and `remind` and
    // `reassign` act on the request's inbox row. Each needs its contract.
    function contractCall(name, args) {
        if (!hasOwn(bindingIds, name)) {
            const error = new NativeError("This operation requires the " + name + " contract");
            define(error, "kind", { value: "unsupported_capability", enumerable: true });
            throw error;
        }
        return invoke(bindingIds[name], args).promise;
    }
    function inputFailure(result) {
        const failure = result.error;
        let kind = "input_failed";
        let message = "input request " + result.status;
        if (result.status === "cancelled") {
            kind = "input_cancelled";
            message = "input request was cancelled";
        } else if (failure !== null && typeof failure === "object" && typeof failure.code === "string") {
            kind = failure.code;
            if (typeof failure.message === "string") message = failure.message;
        } else if (result.status === "failed" && failure === undefined) {
            // The session failed the promise at its hard deadline.
            kind = "deadline_exceeded";
            message = "input request was not answered in time";
        }
        const error = new NativeError(message);
        define(error, "kind", { value: kind, enumerable: true });
        return error;
    }
    function inputHandle(id, promise) {
        let answered = null;
        const answer = function () {
            // One await per handle: every caller shares its outcome.
            if (answered === null) {
                answered = contractCall("promise.await", { promises: [promise] }).then(function (awaited) {
                    const result = awaited.results[0];
                    if (result.status === "resolved") return result.output;
                    throw inputFailure(result);
                });
            }
            return answered;
        };
        const handle = {
            id,
            answer: async function () { return await answer(); },
            remind: async function () { return await contractCall("input.remind", { id }); },
            reassign: async function (args) {
                const to = args === null || typeof args !== "object" ? undefined : args.to;
                return await contractCall("input.reassign", { id, to });
            },
            // Withdraw the request, and wait until it has settled: withdrawn,
            // or answered if the answer came first.
            cancel: async function () {
                cancelHost("promise", promise);
                return await settledStatus(answer(), "input_cancelled");
            },
            // Keep the request open past the run's exits: the session owns
            // it until a later run takes it back with `env.input.handle(id)`.
            // Rejects if the request has already settled; `answer()` has it.
            detach: async function () {
                await contractCall("promise.detach", { promises: [promise] });
                return freeze({ id });
            },
            // Remind every `every_ms` until answered; after `max` reminders
            // (default: no limit) wait, reassign to `to`, or cancel.
            withReminders: async function (options) {
                const settings = options === undefined ? create(null) : options;
                const every = settings.every_ms;
                const max = settings.max === undefined ? Infinity : settings.max;
                const then = settings.then === undefined ? "wait" : settings.then;
                if (typeof every !== "number" || !isFinite(every) || every <= 0) {
                    throw new NativeTypeError("withReminders requires a positive every_ms");
                }
                if (typeof max !== "number" || !(max >= 0)) {
                    throw new NativeTypeError("withReminders max must be a non-negative number");
                }
                if (then !== "wait" && then !== "reassign" && then !== "cancel") {
                    throw new NativeTypeError("withReminders then must be wait, reassign or cancel");
                }
                if (then === "reassign" && !isArray(settings.to)) {
                    throw new NativeTypeError("withReminders then: reassign requires to");
                }
                const outcome = answer().then(function (value) { return { value }; });
                for (let nudges = 0; ; nudges++) {
                    const next = await NativePromise.race([
                        outcome,
                        sleep({ ms: every }).then(function () { return null; }),
                    ]);
                    if (next !== null) return next.value;
                    if (nudges < max) {
                        await handle.remind();
                        continue;
                    }
                    if (then === "reassign") await handle.reassign({ to: settings.to });
                    else if (then === "cancel") await handle.cancel();
                    return (await outcome).value;
                }
            },
        };
        const methods = keys(handle);
        for (let i = 0; i < methods.length; i++) {
            if (typeof handle[methods[i]] === "function") freeze(handle[methods[i]]);
        }
        return freeze(handle);
    }

    // A child agent that did not complete fails its call with its result
    // envelope; one that could not start, with `{ error }`. The program sees
    // `agent_failed`, `agent_cancelled` (cancelled from elsewhere),
    // `deadline_exceeded` or `agent_rejected` as the error's kind, and the
    // document as its value.
    function agentError(document) {
        if (document === null || typeof document !== "object") return null;
        let kind;
        let message;
        if (typeof document.status === "string") {
            kind = "agent_failed";
            if (document.status === "cancelled") kind = "agent_cancelled";
            else if (document.status === "deadline") kind = "deadline_exceeded";
            message = typeof document.error === "string"
                ? document.error
                : "agent " + document.agent + " " + document.status;
        } else if (typeof document.error === "string") {
            kind = "agent_rejected";
            message = document.error;
        } else {
            return null;
        }
        const error = new NativeError(message);
        define(error, "kind", { value: kind, enumerable: true });
        define(error, "value", { value: document, enumerable: true });
        return error;
    }
    function agentCallFailure(error) {
        if (error === null || typeof error !== "object" || error.kind !== "tool_failed") return error;
        let document;
        try {
            document = parse(error.message);
        } catch (_) {
            return error;
        }
        const failure = agentError(document);
        return failure === null ? error : failure;
    }
    // A spawned child agent: `result` awaits its result envelope, `cancel`
    // closes it and waits until it has settled, and `detach` keeps it
    // running past the run's exits (no later run can take it back yet).
    function agentHandle(id, promise) {
        let settled = null;
        const result = function () {
            // One await per handle: every caller shares its outcome.
            if (settled === null) {
                settled = contractCall("promise.await", { promises: [promise] }).then(function (awaited) {
                    const outcome = awaited.results[0];
                    if (outcome.status === "resolved") return outcome.output;
                    let failure = agentError(outcome.error);
                    if (failure === null) {
                        let kind = "agent_failed";
                        let message = "agent " + outcome.status;
                        if (outcome.status === "cancelled") kind = "agent_cancelled";
                        // The session failed the promise at its hard deadline.
                        else if (outcome.error === undefined) kind = "deadline_exceeded";
                        else if (typeof outcome.error === "string") message = outcome.error;
                        failure = new NativeError(message);
                        define(failure, "kind", { value: kind, enumerable: true });
                    }
                    throw failure;
                });
            }
            return settled;
        };
        const handle = {
            id,
            result: async function () { return await result(); },
            cancel: async function () {
                cancelHost("promise", promise);
                return await settledStatus(result(), "agent_cancelled");
            },
            detach: async function () {
                await contractCall("promise.detach", { promises: [promise] });
                return freeze({ id });
            },
        };
        const methods = keys(handle);
        for (let i = 0; i < methods.length; i++) {
            if (typeof handle[methods[i]] === "function") freeze(handle[methods[i]]);
        }
        return freeze(handle);
    }

    // A seeded small fast counter generator (sfc32): every attempt of the
    // same execution draws the same sequence.
    let a = 0, b = 0, c = 0, d = 0;
    function nextUint32() {
        a >>>= 0; b >>>= 0; c >>>= 0; d >>>= 0;
        let t = (a + b) | 0;
        a = b ^ (b >>> 9);
        b = (c + (c << 3)) | 0;
        c = (c << 21) | (c >>> 11);
        d = (d + 1) | 0;
        t = (t + d) | 0;
        c = (c + t) | 0;
        return t >>> 0;
    }

    const NativeDate = Date;
    const nativeParse = NativeDate.parse;
    const nativeUTC = NativeDate.UTC;
    const nativeOffset = NativeDate.prototype.getTimezoneOffset;
    const reflectConstruct = Reflect.construct;
    const days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    function pad(value, width) {
        let text = "" + value;
        while (text.length < width) text = "0" + text;
        return text;
    }
    // Strings without an explicit zone are read as UTC, never in the host's
    // local time zone.
    const zoned = /(?:Z|[+-]\d\d(?::?\d\d)?|\b(?:GMT|UTC|UT)\b.*)$/i;
    const dateOnly = /^[+-]?\d{4,6}(?:-\d\d(?:-\d\d)?)?$/;
    function parseUtc(text) {
        const value = nativeParse(text);
        if (value !== value) return value;
        const trimmed = ("" + text).trim();
        if (zoned.test(trimmed) || dateOnly.test(trimmed)) return value;
        return value - reflectApply(nativeOffset, new NativeDate(value), []) * 60000;
    }
    function hostOwnGlobals() {
        const seed = program.seed;
        a = seed[0]; b = seed[1]; c = seed[2]; d = seed[3];
        for (let i = 0; i < 12; i++) nextUint32();

        function HostDate() {
            const args = arguments;
            if (new.target === undefined) return new HostDate().toString();
            let value;
            if (args.length === 0) value = now;
            else if (args.length === 1) {
                const single = args[0];
                value = typeof single === "string" ? parseUtc(single) : single;
            } else value = reflectApply(nativeUTC, undefined, args);
            return reflectConstruct(NativeDate, [value], new.target);
        }
        define(HostDate, "prototype", { value: NativeDate.prototype });
        define(HostDate, "now", { value: function () { return now; }, writable: true, configurable: true });
        define(HostDate, "parse", { value: parseUtc, writable: true, configurable: true });
        define(HostDate, "UTC", { value: nativeUTC, writable: true, configurable: true });
        const proto = NativeDate.prototype;
        define(proto, "constructor", { value: HostDate, writable: true, configurable: true });
        const local = ["FullYear", "Month", "Date", "Day", "Hours", "Minutes", "Seconds", "Milliseconds"];
        for (let i = 0; i < local.length; i++) {
            const name = local[i];
            define(proto, "get" + name, { value: proto["getUTC" + name], writable: true, configurable: true });
            if (name !== "Day") define(proto, "set" + name, { value: proto["setUTC" + name], writable: true, configurable: true });
        }
        define(proto, "getTimezoneOffset", { value: function () { return 0; }, writable: true, configurable: true });
        const utcDate = function () {
            return days[this.getUTCDay()] + " " + months[this.getUTCMonth()] + " " + pad(this.getUTCDate(), 2) + " " + pad(this.getUTCFullYear(), 4);
        };
        const utcTime = function () {
            return pad(this.getUTCHours(), 2) + ":" + pad(this.getUTCMinutes(), 2) + ":" + pad(this.getUTCSeconds(), 2) + " GMT+0000 (Coordinated Universal Time)";
        };
        const formatters = {
            toString: function () {
                if (this.getTime() !== this.getTime()) return "Invalid Date";
                return utcDate.call(this) + " " + utcTime.call(this);
            },
            toDateString: function () {
                if (this.getTime() !== this.getTime()) return "Invalid Date";
                return utcDate.call(this);
            },
            toTimeString: function () {
                if (this.getTime() !== this.getTime()) return "Invalid Date";
                return utcTime.call(this);
            },
        };
        formatters.toLocaleString = formatters.toString;
        formatters.toLocaleDateString = formatters.toDateString;
        formatters.toLocaleTimeString = formatters.toTimeString;
        const formatterNames = keys(formatters);
        for (let i = 0; i < formatterNames.length; i++) {
            define(proto, formatterNames[i], { value: formatters[formatterNames[i]], writable: true, configurable: true });
        }
        globalThis.Date = HostDate;

        define(Math, "random", {
            value: function () { return nextUint32() / 4294967296; },
            writable: true, configurable: true,
        });
        const integerArrays = [Int8Array, Uint8Array, Uint8ClampedArray, Int16Array, Uint16Array, Int32Array, Uint32Array, BigInt64Array, BigUint64Array];
        const crypto = create(null);
        define(crypto, "getRandomValues", {
            value: function (array) {
                let integer = false;
                for (let i = 0; i < integerArrays.length; i++) {
                    if (array instanceof integerArrays[i]) integer = true;
                }
                if (!integer) throw new NativeTypeError("getRandomValues requires an integer typed array");
                if (array.byteLength > 65536) throw new NativeError("getRandomValues is limited to 65536 bytes");
                const bytes = new Uint8Array(array.buffer, array.byteOffset, array.byteLength);
                for (let i = 0; i < bytes.length; i += 4) {
                    let word = nextUint32();
                    for (let j = 0; j < 4 && i + j < bytes.length; j++) {
                        bytes[i + j] = word & 255;
                        word >>>= 8;
                    }
                }
                return array;
            },
            enumerable: true,
        });
        define(crypto, "randomUUID", {
            value: function () {
                const bytes = crypto.getRandomValues(new Uint8Array(16));
                bytes[6] = (bytes[6] & 15) | 64;
                bytes[8] = (bytes[8] & 63) | 128;
                let hex = "";
                for (let i = 0; i < 16; i++) {
                    hex += pad(bytes[i].toString(16), 2);
                    if (i === 3 || i === 5 || i === 7 || i === 9) hex += "-";
                }
                return hex;
            },
            enumerable: true,
        });
        globalThis.crypto = freeze(crypto);
        if (typeof performance === "object" && performance !== null) {
            globalThis.performance = freeze({
                timeOrigin: program.started_at_ms,
                now: function () { return now - program.started_at_ms; },
            });
        }

        // Timers are durable waits on `time.sleep`; a callback runs once
        // its wait completes, unless cleared first.
        let timerSeq = 0;
        const timers = create(null);
        function schedule(callback, ms, args, repeat) {
            if (typeof callback !== "function") throw new NativeTypeError("Timer callback must be a function");
            if (sleepId === null) throw new NativeError("Timers require the time.sleep contract");
            let delay = +ms;
            if (!(delay >= 0)) delay = 0;
            delay = floor(delay);
            const id = ++timerSeq;
            const tick = function () {
                // The timer holds its current wait, so clearing it cancels
                // the wait as well as the callback.
                const waiting = sleep({ ms: delay });
                timers[id] = waiting;
                waiting.then(function () {
                    if (!timers[id]) return;
                    if (!repeat) delete timers[id];
                    try {
                        reflectApply(callback, undefined, args);
                    } catch (error) {
                        fail(error);
                        return;
                    }
                    if (repeat) tick();
                }, function (error) {
                    if (timers[id]) fail(error);
                });
            };
            tick();
            return id;
        }
        const clear = function (id) {
            const waiting = timers[id];
            if (waiting === undefined) return;
            delete timers[id];
            if (typeof waiting.cancel === "function") waiting.cancel();
        };
        globalThis.setTimeout = function (callback, ms) {
            return schedule(callback, ms, reflectApply(arraySlice, arguments, [2]), false);
        };
        globalThis.setInterval = function (callback, ms) {
            return schedule(callback, ms, reflectApply(arraySlice, arguments, [2]), true);
        };
        globalThis.clearTimeout = clear;
        globalThis.clearInterval = clear;
    }
    // A timer callback that throws fails the program, as an uncaught
    // exception would.
    let failProgram = null;
    let pendingFailure = null;
    function fail(error) {
        if (failProgram !== null) failProgram(error);
        else if (pendingFailure === null) pendingFailure = { error };
    }
    // Before any module code runs: build `env` and `ctx` from intrinsics the
    // module cannot have touched yet, and stop code generation from strings.
    // Each function constructor is replaced by one that refuses but keeps
    // the original prototype, so `instanceof Function` still holds.
    let programEnv = null;
    let programCtx = null;
    // The two exits. `ctx.complete(result, state)` ends the run with
    // `result` and hands `state` to the next run; `ctx.continueAsNew(state)`
    // goes on with the same run in a new execution that starts from `state`.
    // Values are copied when called. Each returns a token that takes effect
    // only when `run` returns it, so a `catch` cannot swallow it; calling
    // one without returning the token, or more than once, fails the run.
    const exitToken = freeze(create(null));
    let exit = null;
    let exits = 0;
    function exitWith(kind, value, state) {
        const stateJson = serialize(state);
        keep(stateJson, null);
        exits++;
        exit = { kind, value, stateJson };
        return exitToken;
    }
    function complete(result, state) {
        const value = result === undefined ? undefined : parse(serialize(result));
        return exitWith("complete", value, state);
    }
    function continueAsNew(state) {
        return exitWith("continueAsNew", undefined, state);
    }
    // Whether the run should go on in a new execution: its history is
    // getting long. The host fixes the answer for each step from its
    // history, so a replay sees the same.
    function shouldContinue() {
        return shouldContinueHost() === true;
    }
    function prepare() {
        programEnv = buildEnv();
        // The state the last completed run, or this run's previous
        // execution, handed over: a fresh copy for this execution.
        programCtx = freeze({
            startedAt: program.started_at_ms,
            state: hasOwn(program, "state") ? program.state : undefined,
            continued: program.continued === true,
            complete: freeze(complete),
            continueAsNew: freeze(continueAsNew),
            shouldContinue: freeze(shouldContinue),
        });
        harden();
    }
    function harden() {
        function refusing(original) {
            const refuse = function () {
                throw new NativeError("Code generation from strings is disabled in programs");
            };
            define(refuse, "prototype", { value: original.prototype, writable: false, configurable: false });
            define(original.prototype, "constructor", { value: refuse, writable: false, configurable: false });
            return refuse;
        }
        const tamedFunction = refusing(Function);
        refusing(AsyncFunction);
        refusing((function* () {}).constructor);
        refusing((async function* () {}).constructor);
        define(globalThis, "Function", { value: tamedFunction, writable: false, configurable: false });
        define(globalThis, "eval", { value: tamedFunction, writable: false, configurable: false });
    }
    hostOwnGlobals();

    return {
        program: function (namespace, evaluation) {
            const env = programEnv;
            const ctx = programCtx;
            const input = program.input;
            const failure = new NativePromise(function (_, reject) {
                failProgram = reject;
                if (pendingFailure !== null) reject(pendingFailure.error);
            });
            const main = (async function () {
                await evaluation;
                const handler = namespace.default;
                if (handler === null || typeof handler !== "object" || typeof handler.run !== "function") {
                    throw new NativeTypeError("A program's default export must be an object with a run(input, env, ctx) method");
                }
                const returned = await handler.run(input, env, ctx);
                if (exit === null) return returned;
                const name = "ctx." + exit.kind + "()";
                if (returned !== exitToken) {
                    throw new NativeError(name + " was called but run did not return its result");
                }
                if (exits > 1) throw new NativeError("ctx.complete() or ctx.continueAsNew() was called more than once");
                keep(exit.stateJson, exit.kind);
                return exit.value;
            })();
            return NativePromise.race([main, failure]);
        },
        prepare,
        serialize,
        deliver: function (id, success, json, at) {
            const pair = pending[id];
            if (!pair) throw new NativeError("Unknown host completion");
            delete pending[id];
            advance(at);
            const value = parse(json);
            if (success) pair.resolve(value);
            else {
                const error = new NativeError(value.message);
                define(error, "kind", { value: value.kind, enumerable: true });
                define(error, "value", { value: value.value, enumerable: true });
                pair.reject(error);
            }
        }
    };
})
