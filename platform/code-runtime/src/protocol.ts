// Internal protocol between the execution workflow and the `runScript`
// activity that drives one sandbox generation.

import type {
  ExecutionError,
  ExecutionLimits,
  Json,
  JournalEntry,
  LogEntry,
} from "./contract.js";

/** Registers calls the sandbox issued. */
export const ISSUE_UPDATE = "issue";
/** Waits for recorded results past a position, or tells the sandbox to suspend. */
export const NEXT_UPDATE = "next";
/** Reads recorded results. A query, so large values are not re-recorded. */
export const JOURNAL_QUERY = "journal";

export const STALE_GENERATION = "StaleGeneration";

export interface IssuedCall {
  key: string;
  tool: string;
  arguments: Json;
}

export interface IssueRequest {
  generation: number;
  calls: IssuedCall[];
}

export type IssueResponse =
  | { status: "accepted" }
  | { status: "diverged"; message: string };

export interface NextRequest {
  generation: number;
  /** Number of journal entries already delivered to the sandbox. */
  after: number;
}

export type NextResponse =
  | { status: "available"; total: number }
  | { status: "suspend" };

export interface JournalPageRequest {
  after: number;
  max_bytes: number;
}

export interface JournalPage {
  entries: JournalEntry[];
  total: number;
}

export interface RunScriptInput {
  generation: number;
  source: string;
  input: Json;
  tool_names: string[];
  seed: [number, number];
  start_clock: number;
  limits: ExecutionLimits;
}

export type RunScriptOutput =
  | {
      status: "completed";
      output: Json;
      logs: LogEntry[];
      logs_truncated: boolean;
    }
  | {
      status: "failed";
      error: ExecutionError;
      logs: LogEntry[];
      logs_truncated: boolean;
    }
  | {
      /** Every outstanding call outlived its hold; the sandbox was discarded. */
      status: "suspended";
      delivered: number;
      /** Everything the script logged, from its start, in this generation. */
      logs: LogEntry[];
      logs_truncated: boolean;
    };
