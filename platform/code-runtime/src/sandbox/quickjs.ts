import {
  getQuickJS,
  type QuickJSContext,
  type QuickJSDeferredPromise,
  type QuickJSHandle,
  type QuickJSRuntime,
} from "quickjs-emscripten";
import { byteLength, type CallOutcome, type ExecutionError, type LogEntry } from "../contract.js";
import { PRELUDE } from "./prelude.js";
import type {
  Sandbox,
  SandboxCall,
  SandboxOptions,
  SandboxProgress,
  SandboxSession,
} from "./types.js";

const STACK_BYTES = 512 * 1024;
const LOG_LEVELS = new Set(["debug", "info", "warn", "error"]);
const TIMEOUT: ExecutionError = {
  code: "generation_timeout",
  message: "the script ran past generation_timeout_ms without awaiting a call",
};

// The interrupt surfaces as an InternalError the script cannot catch. It
// rejects the script's own promise when it fires inside the main function.
function isInterrupt(error: { name: string; message: string }): boolean {
  return error.name === "InternalError" && error.message.includes("interrupted");
}

/**
 * In-process QuickJS sandbox. The context has no host APIs beyond the four
 * functions the prelude consumes, and the host decides exactly when pending
 * jobs run, so a script's behaviour is a function of the results delivered.
 */
export class QuickJsSandbox implements Sandbox {
  async start(options: SandboxOptions): Promise<SandboxSession> {
    const quickjs = await getQuickJS();
    const runtime = quickjs.newRuntime();
    runtime.setMemoryLimit(options.memoryBytes);
    runtime.setMaxStackSize(STACK_BYTES);
    const session = new QuickJsSession(runtime, runtime.newContext(), options);
    try {
      session.boot();
    } catch (error) {
      session.dispose();
      throw error;
    }
    return session;
  }
}

class QuickJsSession implements SandboxSession {
  private readonly pending = new Map<number, QuickJSDeferredPromise>();
  private issued: SandboxCall[] = [];
  private nextId = 0;
  private clock: number;
  private done: { ok: boolean; json: string } | undefined;
  private bootError: ExecutionError | undefined;
  private readonly logEntries: LogEntry[] = [];
  private logBytes = 0;
  private logsTruncated = false;
  private disposed = false;

  constructor(
    private readonly runtime: QuickJSRuntime,
    private readonly vm: QuickJSContext,
    private readonly options: SandboxOptions,
  ) {
    this.clock = options.startClock;
    runtime.setInterruptHandler(() => Date.now() > options.deadline);
  }

  boot(): void {
    const { vm, options } = this;
    this.defineFunction("__hostCall", (name, args) => {
      const deferred = vm.newPromise();
      const id = this.nextId++;
      this.pending.set(id, deferred);
      this.issued.push({ id, tool: vm.getString(name), argumentsJson: vm.getString(args) });
      return deferred.handle;
    });
    this.defineFunction("__hostLog", (level, text) => {
      this.appendLog(vm.getString(level), vm.getString(text));
      return vm.undefined;
    });
    this.defineFunction("__hostClock", () => vm.newNumber(this.clock));
    this.defineFunction("__hostDone", (ok, json) => {
      this.done ??= { ok: vm.dump(ok) === true, json: vm.getString(json) };
      return vm.undefined;
    });
    this.defineValue("__toolNames", vm.newString(JSON.stringify(options.toolNames)));
    this.defineValue("__inputJson", vm.newString(JSON.stringify(options.input ?? null)));
    this.defineValue("__seed0", vm.newNumber(options.seed[0]));
    this.defineValue("__seed1", vm.newNumber(options.seed[1]));

    const prelude = this.evaluate(PRELUDE, "prelude.js");
    if (prelude) throw new Error(`sandbox prelude failed: ${prelude.message}`);
    this.bootError = this.evaluate(`${options.code}\n;__start(__main);`, "script.js");
  }

  async run(): Promise<SandboxProgress> {
    if (this.bootError) return { status: "failed", error: this.bootError };
    const result = this.runtime.executePendingJobs();
    if (result.error) {
      const error = this.describe(result.error);
      result.error.dispose();
      return { status: "failed", error };
    }
    const issued = this.issued;
    this.issued = [];
    if (!this.done) return { status: "pending", issued };
    if (this.done.ok) return { status: "completed", issued, outputJson: this.done.json };
    const thrown = JSON.parse(this.done.json) as { name: string; message: string; stack: string };
    if (Date.now() > this.options.deadline && isInterrupt(thrown)) {
      return { status: "failed", error: TIMEOUT };
    }
    return {
      status: "failed",
      error: {
        code: "script_error",
        message: `${thrown.name}: ${thrown.message}`,
        ...(thrown.stack ? { stack: thrown.stack } : {}),
      },
    };
  }

  deliver(id: number, outcome: CallOutcome, clock: number): void {
    const deferred = this.pending.get(id);
    if (!deferred) throw new Error(`sandbox has no pending call ${id}`);
    this.pending.delete(id);
    this.clock = clock;
    const value = this.vm.newString(JSON.stringify(outcome));
    deferred.resolve(value);
    value.dispose();
    deferred.dispose();
  }

  logs(): { entries: LogEntry[]; truncated: boolean } {
    return { entries: this.logEntries, truncated: this.logsTruncated };
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    for (const deferred of this.pending.values()) deferred.dispose();
    this.pending.clear();
    this.vm.dispose();
    try {
      this.runtime.dispose();
    } catch {
      // A script abandoned mid-run can leave values the runtime reports as
      // leaked. The WebAssembly memory is released with the module either way.
    }
  }

  private defineFunction(
    name: string,
    implementation: (...args: QuickJSHandle[]) => QuickJSHandle,
  ): void {
    this.defineValue(name, this.vm.newFunction(name, implementation));
  }

  private defineValue(name: string, handle: QuickJSHandle): void {
    this.vm.setProp(this.vm.global, name, handle);
    handle.dispose();
  }

  private evaluate(code: string, filename: string): ExecutionError | undefined {
    const result = this.vm.evalCode(code, filename);
    if (result.error) {
      const error = this.describe(result.error);
      result.error.dispose();
      return error;
    }
    result.value.dispose();
    return undefined;
  }

  private describe(handle: QuickJSHandle): ExecutionError {
    const dumped = this.vm.dump(handle) as unknown;
    const fields =
      typeof dumped === "object" && dumped !== null
        ? (dumped as { name?: unknown; message?: unknown; stack?: unknown })
        : { message: dumped };
    const message = String(fields.message ?? "unknown error");
    if (isInterrupt({ name: String(fields.name), message })) return TIMEOUT;
    return {
      code: "script_error",
      message: `${String(fields.name ?? "Error")}: ${message}`,
      ...(typeof fields.stack === "string" && fields.stack ? { stack: fields.stack } : {}),
    };
  }

  private appendLog(level: string, text: string): void {
    if (this.logsTruncated) return;
    const bytes = byteLength(text);
    if (this.logBytes + bytes > this.options.maxLogBytes) {
      this.logsTruncated = true;
      return;
    }
    this.logBytes += bytes;
    this.logEntries.push({
      level: (LOG_LEVELS.has(level) ? level : "info") as LogEntry["level"],
      text,
    });
  }
}
