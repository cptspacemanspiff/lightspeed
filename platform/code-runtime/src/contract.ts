// The runtime's public contract. Everything here is plain data so it can be
// imported from workflow code, activities, and hosts alike.

export const CODE_RUNTIME_TASK_QUEUE = "code-runtime";
export const CODE_EXECUTION_WORKFLOW = "CodeExecutionWorkflow";

/** Signal a `host` target receives for each call it must execute. */
export const CALL_REQUESTED_SIGNAL = "call_requested";
/** Signal a `host` target sends back with the call's outcome. */
export const CALL_COMPLETED_SIGNAL = "call_completed";

/** Built-in durable sleep, exposed to scripts as `sleep(ms)`. */
export const SLEEP_TOOL = "sleep";

export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json };

export type JsonSchema = { [key: string]: Json } | boolean;

export interface CodeExecutionInput {
  /** TypeScript async function body. `input` and `tools` are in scope. */
  source: string;
  input: Json;
  tools: ToolManifestEntry[];
  limits?: Partial<ExecutionLimits>;
  /** Validated against the script's return value. */
  output_schema?: JsonSchema;
}

export interface ToolManifestEntry {
  /** Dotted path; `web.fetch` becomes `tools.web.fetch(args)`. */
  name: string;
  description?: string;
  input_schema: JsonSchema;
  /** Validated on every successful result when present. */
  output_schema?: JsonSchema;
  target: ToolTarget;
  /**
   * How long a sandbox waits for this call before it is discarded and the
   * script is later re-run against the recorded result. Defaults to 5000.
   */
  hold_ms?: number;
  /** Opaque to the runtime; passed to the target unchanged. */
  context?: Json;
}

export type ToolTarget =
  | {
      type: "activity";
      task_queue: string;
      activity_type: string;
      timeout_ms: number;
      max_attempts?: number;
    }
  | {
      type: "workflow";
      task_queue: string;
      workflow_type: string;
      timeout_ms: number;
    }
  | {
      /** Request and reply by signal with an existing workflow. */
      type: "host";
      workflow_id: string;
      timeout_ms: number;
    };

/** What an `activity` or `workflow` target receives as its one argument. */
export interface ToolCallRequest {
  execution_id: string;
  call_id: string;
  tool: string;
  arguments: Json;
  context: Json;
}

/** Payload of `call_completed` from a `host` target. */
export interface HostCallCompleted {
  call_id: string;
  outcome: CallOutcome;
}

/**
 * `ApplicationFailure` type a target throws to fail one call with an error
 * the script can catch. Any other failure is reported as `target_failed`.
 */
export const TOOL_ERROR_TYPE = "ToolError";

export interface ExecutionLimits {
  max_calls: number;
  max_concurrent_calls: number;
  max_generations: number;
  generation_timeout_ms: number;
  deadline_ms: number;
  max_source_bytes: number;
  max_value_bytes: number;
  max_log_bytes: number;
  sandbox_memory_bytes: number;
}

export const DEFAULT_LIMITS: ExecutionLimits = {
  max_calls: 200,
  max_concurrent_calls: 16,
  max_generations: 50,
  generation_timeout_ms: 30_000,
  deadline_ms: 3_600_000,
  max_source_bytes: 256 * 1024,
  max_value_bytes: 512 * 1024,
  max_log_bytes: 64 * 1024,
  sandbox_memory_bytes: 64 * 1024 * 1024,
};

export const DEFAULT_HOLD_MS = 5_000;

export type CallErrorCode =
  /** The tool is not in the manifest. */
  | "unknown_tool"
  /** Arguments failed the tool's input schema. */
  | "invalid_arguments"
  /** The result failed the tool's output schema. */
  | "invalid_result"
  /** Arguments or result exceeded `max_value_bytes`. */
  | "value_too_large"
  /** The execution exceeded `max_calls`. */
  | "call_limit"
  /** The target raised a tool error. */
  | "tool_error"
  /** The target failed, timed out, or was cancelled. */
  | "target_failed";

export interface CallError {
  code: CallErrorCode;
  message: string;
  details?: Json;
}

export type CallOutcome =
  | { ok: true; value: Json }
  | { ok: false; error: CallError };

export type ExecutionErrorCode =
  | "invalid_input"
  | "invalid_source"
  | "script_error"
  | "invalid_output"
  | "value_too_large"
  | "diverged"
  | "stalled"
  | "generation_timeout"
  | "generation_limit"
  | "deadline_exceeded"
  | "sandbox_failed";

export interface ExecutionError {
  code: ExecutionErrorCode;
  message: string;
  /** Script stack for `script_error`. */
  stack?: string;
}

export interface LogEntry {
  level: "debug" | "info" | "warn" | "error";
  text: string;
}

export interface ExecutionStats {
  generations: number;
  calls: number;
  rejected_calls: number;
}

export interface CodeExecutionResult {
  status: "completed" | "failed";
  output?: Json;
  error?: ExecutionError;
  logs: LogEntry[];
  logs_truncated: boolean;
  stats: ExecutionStats;
}

/** One completed call, in completion order. */
export interface JournalEntry {
  seq: number;
  key: string;
  call_id: string;
  tool: string;
  outcome: CallOutcome;
  /** Workflow time when the result was recorded; the script's clock. */
  clock: number;
}

export function byteLength(text: string): number {
  // UTF-8 length without TextEncoder, which workflow code cannot rely on.
  let bytes = 0;
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i);
    if (code < 0x80) bytes += 1;
    else if (code < 0x800) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff) {
      bytes += 4;
      i++;
    } else bytes += 3;
  }
  return bytes;
}
