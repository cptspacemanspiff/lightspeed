import { spawn, type ChildProcess } from "node:child_process";
import type { Readable } from "node:stream";
import workerd from "workerd";
import { byteLength, type CallOutcome, type ExecutionError, type LogEntry } from "../../contract.js";
import type {
  Sandbox,
  SandboxCall,
  SandboxOptions,
  SandboxProgress,
  SandboxSession,
} from "../types.js";
import { configPath } from "./config.js";
import { scriptModule } from "./module.js";

const STARTUP_TIMEOUT_MS = 10_000;
const LOG_LEVELS = new Set(["debug", "info", "warn", "error"]);

// A sandbox must not outlive the worker process that owns it.
const children = new Set<ChildProcess>();
process.once("exit", () => {
  for (const child of children) child.kill("SIGKILL");
});

export interface WorkerdSandboxOptions {
  /** Path to the workerd binary. Defaults to the one the npm package installed. */
  binaryPath?: string;
}

type ExecutorMessage =
  | {
      type: "step";
      issued: SandboxCall[];
      logs: { level: string; text: string }[];
      done?: { ok: boolean; json: string };
    }
  | { type: "error"; name: string; message: string; stack: string }
  | { type: "closed"; reason: string };

/**
 * Runs each script in its own workerd process: a fresh V8 isolate created
 * through the Worker Loader binding, with no network access. A process per
 * session means a script that never yields can simply be killed, and takes
 * nothing else down with it.
 */
export class WorkerdSandbox implements Sandbox {
  private readonly binaryPath: string;

  constructor(options: WorkerdSandboxOptions = {}) {
    this.binaryPath =
      options.binaryPath ?? (typeof workerd === "string" ? workerd : workerd.default);
  }

  async start(options: SandboxOptions): Promise<SandboxSession> {
    const child = spawn(
      this.binaryPath,
      [
        "serve",
        configPath(options.memoryBytes),
        "--experimental",
        "--control-fd=3",
        "--socket-addr",
        "http=127.0.0.1:0",
      ],
      // The process inherits nothing from the worker's environment.
      { env: {}, stdio: ["ignore", "ignore", "pipe", "pipe"] },
    );
    children.add(child);
    child.once("exit", () => children.delete(child));
    try {
      const port = await listeningPort(child);
      const socket = await connect(`ws://127.0.0.1:${port}/`);
      return new WorkerdSession(child, socket, options);
    } catch (error) {
      child.kill("SIGKILL");
      throw error;
    }
  }
}

class WorkerdSession implements SandboxSession {
  private readonly inbox: ExecutorMessage[] = [];
  private wake: (() => void) | undefined;
  private readonly deliveries: string[] = [];
  /** The isolate has reported a step and is waiting for one result. */
  private awaitingDelivery = false;
  private readonly logEntries: LogEntry[] = [];
  private logBytes = 0;
  private logsTruncated = false;
  private disposed = false;

  constructor(
    private readonly child: ChildProcess,
    private readonly socket: WebSocket,
    private readonly options: SandboxOptions,
  ) {
    const push = (message: ExecutorMessage): void => {
      this.inbox.push(message);
      this.wake?.();
    };
    socket.addEventListener("message", (event) => {
      push(JSON.parse(String(event.data)) as ExecutorMessage);
    });
    socket.addEventListener("close", () => push({ type: "closed", reason: "connection closed" }));
    child.once("exit", (code, signal) =>
      push({ type: "closed", reason: `workerd exited (${signal ?? code})` }),
    );
    socket.send(
      JSON.stringify({
        type: "start",
        module: scriptModule(options.code),
        init: {
          inputJson: JSON.stringify(options.input ?? null),
          toolNames: options.toolNames,
          seed: options.seed,
          startClock: options.startClock,
        },
      }),
    );
  }

  async run(): Promise<SandboxProgress> {
    if (this.awaitingDelivery) return { status: "pending", issued: [] };
    const message = await this.receive();
    if (!message) {
      // Still running past the deadline: the script is not yielding.
      this.dispose();
      return { status: "failed", error: TIMEOUT };
    }
    // Also how a script that exhausts its heap ends: V8 aborts the process.
    if (message.type === "closed") throw new Error(`sandbox lost: ${message.reason}`);
    if (message.type === "error") {
      return { status: "failed", error: scriptError(message) };
    }

    for (const entry of message.logs) this.appendLog(entry.level, entry.text);
    if (!message.done) {
      this.awaitingDelivery = true;
      this.flush();
      return { status: "pending", issued: message.issued };
    }
    if (message.done.ok) {
      return { status: "completed", issued: message.issued, outputJson: message.done.json };
    }
    return {
      status: "failed",
      error: scriptError(JSON.parse(message.done.json) as { name: string; message: string; stack: string }),
    };
  }

  deliver(id: number, outcome: CallOutcome, clock: number): void {
    this.deliveries.push(
      JSON.stringify({ type: "deliver", reply: { id, outcomeJson: JSON.stringify(outcome), clock } }),
    );
    this.flush();
  }

  logs(): { entries: LogEntry[]; truncated: boolean } {
    return { entries: this.logEntries, truncated: this.logsTruncated };
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.child.kill("SIGKILL");
    try {
      this.socket.close();
    } catch {
      // Already closed with the process.
    }
  }

  /** The isolate takes one result per step, so later ones wait their turn. */
  private flush(): void {
    if (!this.awaitingDelivery) return;
    const next = this.deliveries.shift();
    if (next === undefined) return;
    this.awaitingDelivery = false;
    this.socket.send(next);
  }

  private async receive(): Promise<ExecutorMessage | undefined> {
    for (;;) {
      const message = this.inbox.shift();
      if (message) return message;
      const remaining = this.options.deadline - Date.now();
      if (remaining <= 0) return undefined;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, remaining);
        this.wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
      this.wake = undefined;
    }
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

const TIMEOUT: ExecutionError = {
  code: "generation_timeout",
  message: "the script ran past generation_timeout_ms without awaiting a call",
};

function scriptError(error: { name: string; message: string; stack: string }): ExecutionError {
  return {
    code: "script_error",
    message: `${error.name}: ${error.message}`,
    ...(error.stack ? { stack: error.stack } : {}),
  };
}

/** workerd reports the port it bound as a JSON line on the control descriptor. */
function listeningPort(child: ChildProcess): Promise<number> {
  return new Promise((resolve, reject) => {
    const control = child.stdio[3] as Readable;
    let buffered = "";
    let diagnostics = "";
    const timer = setTimeout(
      () => reject(new Error("workerd did not start listening in time")),
      STARTUP_TIMEOUT_MS,
    );
    child.stderr?.on("data", (chunk: Buffer) => {
      diagnostics = (diagnostics + chunk.toString()).slice(-2_000);
    });
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      reject(new Error(`workerd exited during startup (${signal ?? code}): ${diagnostics.trim()}`));
    });
    control.on("data", (chunk: Buffer) => {
      buffered += chunk.toString();
      for (;;) {
        const newline = buffered.indexOf("\n");
        if (newline < 0) return;
        const line = buffered.slice(0, newline);
        buffered = buffered.slice(newline + 1);
        if (!line) continue;
        const event = JSON.parse(line) as { event?: string; port?: number };
        if (event.event === "listen" && typeof event.port === "number") {
          clearTimeout(timer);
          resolve(event.port);
        }
      }
    });
  });
}

function connect(url: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    socket.addEventListener("open", () => resolve(socket), { once: true });
    socket.addEventListener("error", () => reject(new Error("could not connect to workerd")), {
      once: true,
    });
  });
}
