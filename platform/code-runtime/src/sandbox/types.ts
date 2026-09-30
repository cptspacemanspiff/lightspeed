import type { CallOutcome, ExecutionError, Json, LogEntry } from "../contract.js";

export interface SandboxOptions {
  /** Compiled JavaScript declaring `async function __main(input)`. */
  code: string;
  input: Json;
  toolNames: string[];
  seed: [number, number];
  startClock: number;
  memoryBytes: number;
  maxLogBytes: number;
  /** Wall-clock time after which running script code is interrupted. */
  deadline: number;
}

export interface SandboxCall {
  /** Identifies the call within this sandbox session only. */
  id: number;
  tool: string;
  argumentsJson: string;
}

export type SandboxProgress =
  | { status: "pending"; issued: SandboxCall[] }
  | { status: "completed"; issued: SandboxCall[]; outputJson: string }
  | { status: "failed"; error: ExecutionError };

/**
 * One disposable run of a script. The host alternates `run`, which executes
 * the script until it can make no progress without a result, and `deliver`,
 * which hands it one result. The script runs to rest after each result
 * before it sees the next, so its behaviour depends only on their order.
 */
export interface SandboxSession {
  run(): Promise<SandboxProgress>;
  deliver(id: number, outcome: CallOutcome, clock: number): void;
  logs(): { entries: LogEntry[]; truncated: boolean };
  dispose(): void;
}

export interface Sandbox {
  start(options: SandboxOptions): Promise<SandboxSession>;
}
