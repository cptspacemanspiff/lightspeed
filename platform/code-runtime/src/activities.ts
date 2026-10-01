import { createHash } from "node:crypto";
import { Context } from "@temporalio/activity";
import type { Client } from "@temporalio/client";
import { byteLength, type Json, type JournalEntry } from "./contract.js";
import {
  ISSUE_UPDATE,
  JOURNAL_QUERY,
  NEXT_UPDATE,
  type IssueRequest,
  type IssueResponse,
  type IssuedCall,
  type JournalPage,
  type JournalPageRequest,
  type NextRequest,
  type NextResponse,
  type RunScriptInput,
  type RunScriptOutput,
} from "./protocol.js";
import type { Sandbox } from "./sandbox/types.js";
import { WorkerdSandbox } from "./sandbox/workerd/sandbox.js";
import { compileSource } from "./source.js";

const HEARTBEAT_MS = 5_000;
// Temporal rejects payloads near 2 MiB; stay well under it per message.
const MESSAGE_BYTES = 1024 * 1024;

export interface ActivityDependencies {
  /** Used to reach the execution workflow that started the activity. */
  client: Client;
  /** Defaults to workerd. */
  sandbox?: Sandbox;
}

export function createActivities(dependencies: ActivityDependencies) {
  const sandbox = dependencies.sandbox ?? new WorkerdSandbox();
  return {
    runScript: (input: RunScriptInput) => runScript(dependencies.client, sandbox, input),
  };
}

/**
 * Run one sandbox generation. The script starts from the top every time;
 * recorded results are handed back in the order they were recorded, so it
 * retraces the earlier generations' path and then continues live.
 */
async function runScript(
  client: Client,
  sandbox: Sandbox,
  input: RunScriptInput,
): Promise<RunScriptOutput> {
  const compiled = compileSource(input.source);
  if (!compiled.ok) {
    return {
      status: "failed",
      error: { code: "invalid_source", message: compiled.message },
      logs: [],
      logs_truncated: false,
    };
  }

  const context = Context.current();
  const execution = context.info.workflowExecution;
  if (!execution) throw new Error("runScript must be started by an execution workflow");
  const { workflowId, runId } = execution;
  const workflow = client.workflow.getHandle(workflowId, runId);
  const cancellable = <T>(operation: Promise<T>): Promise<T> =>
    Promise.race([operation, context.cancelled]);

  const deadline = Date.now() + input.limits.generation_timeout_ms;
  const session = await sandbox.start({
    code: compiled.code,
    input: input.input,
    toolNames: input.tool_names,
    seed: input.seed,
    startClock: input.start_clock,
    memoryBytes: input.limits.sandbox_memory_bytes,
    maxLogBytes: input.limits.max_log_bytes,
    deadline,
  });
  const heartbeat = setInterval(() => context.heartbeat(), HEARTBEAT_MS);

  const suspended = (delivered: number): RunScriptOutput => {
    const logs = session.logs();
    return { status: "suspended", delivered, logs: logs.entries, logs_truncated: logs.truncated };
  };
  const finished = (fields: { status: "completed"; output: Json } | FailedFields): RunScriptOutput => {
    const logs = session.logs();
    return { ...fields, logs: logs.entries, logs_truncated: logs.truncated };
  };

  try {
    const journal: JournalEntry[] = [];
    const outstanding = new Map<string, number>();
    const occurrences = new Map<string, number>();
    let total = 0;
    let delivered = 0;
    let synced = false;

    const fetchJournal = async (): Promise<void> => {
      const request: JournalPageRequest = { after: journal.length, max_bytes: MESSAGE_BYTES };
      const page = await cancellable(workflow.query<JournalPage, [JournalPageRequest]>(JOURNAL_QUERY, request));
      journal.push(...page.entries);
      total = page.total;
      synced = true;
    };

    for (;;) {
      const progress = await session.run();
      if (progress.status === "failed") return finished({ status: "failed", error: progress.error });

      const issued: IssuedCall[] = progress.issued.map((call) => {
        const args = JSON.parse(call.argumentsJson) as Json;
        const identity = `${call.tool}:${hash(canonical(args))}`;
        const occurrence = occurrences.get(identity) ?? 0;
        occurrences.set(identity, occurrence + 1);
        const key = `${identity}:${occurrence}`;
        outstanding.set(key, call.id);
        return { key, tool: call.tool, arguments: args };
      });
      for (const batch of batches(issued)) {
        const request: IssueRequest = { generation: input.generation, calls: batch };
        const response = await cancellable(
          workflow.executeUpdate<IssueResponse, [IssueRequest]>(ISSUE_UPDATE, { args: [request] }),
        );
        if (response.status === "diverged") {
          return finished({ status: "failed", error: { code: "diverged", message: response.message } });
        }
      }

      if (progress.status === "completed") {
        if (byteLength(progress.outputJson) > input.limits.max_value_bytes) {
          return finished({
            status: "failed",
            error: { code: "value_too_large", message: "the script's result exceeds max_value_bytes" },
          });
        }
        return finished({ status: "completed", output: JSON.parse(progress.outputJson) as Json });
      }

      if (!synced || (delivered === journal.length && delivered < total)) await fetchJournal();
      const entry = journal[delivered];
      if (entry) {
        const id = outstanding.get(entry.key);
        if (id === undefined) {
          // The recorded run had made this call by now. This run has not.
          return finished({
            status: "failed",
            error: {
              code: "diverged",
              message: `the script did not repeat an earlier call to ${entry.tool}`,
            },
          });
        }
        outstanding.delete(entry.key);
        session.deliver(id, entry.outcome, entry.clock);
        delivered += 1;
        continue;
      }

      if (outstanding.size === 0) {
        return finished({
          status: "failed",
          error: {
            code: "stalled",
            message: "the script is waiting on something that is not a tool call",
          },
        });
      }
      if (Date.now() >= deadline) return suspended(delivered);
      const request: NextRequest = { generation: input.generation, after: delivered };
      const response = await cancellable(
        workflow.executeUpdate<NextResponse, [NextRequest]>(NEXT_UPDATE, { args: [request] }),
      );
      if (response.status === "suspend") return suspended(delivered);
      total = response.total;
    }
  } finally {
    clearInterval(heartbeat);
    session.dispose();
  }
}

type FailedFields = Pick<Extract<RunScriptOutput, { status: "failed" }>, "status" | "error">;

function* batches(calls: IssuedCall[]): Generator<IssuedCall[]> {
  let batch: IssuedCall[] = [];
  let bytes = 0;
  for (const call of calls) {
    const size = byteLength(JSON.stringify(call));
    if (batch.length > 0 && bytes + size > MESSAGE_BYTES) {
      yield batch;
      batch = [];
      bytes = 0;
    }
    batch.push(call);
    bytes += size;
  }
  if (batch.length > 0) yield batch;
}

/** JSON with object keys sorted, so equal arguments hash equally. */
function canonical(value: Json): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  const fields = Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonical(value[key] ?? null)}`);
  return `{${fields.join(",")}}`;
}

function hash(text: string): string {
  return createHash("sha256").update(text).digest("hex").slice(0, 32);
}
