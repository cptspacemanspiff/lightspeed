// Workflow code: deterministic, no Node APIs.
//
// The workflow owns the journal. A sandbox generation can only ask it to
// register calls and to wait for results; the workflow validates, dispatches,
// and records every call, so a discarded or crashed sandbox loses nothing.

import {
  ActivityFailure,
  ApplicationFailure,
  ChildWorkflowFailure,
  ParentClosePolicy,
  condition,
  defineQuery,
  defineSignal,
  defineUpdate,
  executeChild,
  getExternalWorkflowHandle,
  isCancellation,
  proxyActivities,
  setHandler,
  sleep,
  workflowInfo,
} from "@temporalio/workflow";
import { HandlerUnfinishedPolicy } from "@temporalio/common";
import { Validator } from "@cfworker/json-schema";
import {
  CALL_COMPLETED_SIGNAL,
  CALL_REQUESTED_SIGNAL,
  DEFAULT_HOLD_MS,
  DEFAULT_LIMITS,
  DRIVER_RESULTS_SIGNAL,
  DRIVER_STATE_QUERY,
  SLEEP_TOOL,
  TOOL_ERROR_TYPE,
  byteLength,
  type CallError,
  type CallOutcome,
  type CodeExecutionInput,
  type CodeExecutionResult,
  type DriverResults,
  type DriverState,
  type DriverStep,
  type ExecutionError,
  type ExecutionLimits,
  type HostCallCompleted,
  type Json,
  type JsonSchema,
  type JournalEntry,
  type LogEntry,
  type ToolCallRequest,
  type ToolManifestEntry,
} from "./contract.js";
import {
  ISSUE_UPDATE,
  JOURNAL_QUERY,
  NEXT_UPDATE,
  STALE_GENERATION,
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

const TOOL_NAME = /^[A-Za-z_$][\w$]*(\.[A-Za-z_$][\w$]*)*$/;
const MAX_SANDBOX_FAILURES = 3;

const SLEEP_SCHEMA: JsonSchema = {
  type: "object",
  properties: { ms: { type: "integer", minimum: 0 } },
  required: ["ms"],
  additionalProperties: false,
};

const issueUpdate = defineUpdate<IssueResponse, [IssueRequest]>(ISSUE_UPDATE);
const nextUpdate = defineUpdate<NextResponse, [NextRequest]>(NEXT_UPDATE);
const journalQuery = defineQuery<JournalPage, [JournalPageRequest]>(JOURNAL_QUERY);
const callCompletedSignal = defineSignal<[HostCallCompleted]>(CALL_COMPLETED_SIGNAL);
const driverResultsSignal = defineSignal<[DriverResults]>(DRIVER_RESULTS_SIGNAL);
const driverStateQuery = defineQuery<DriverState>(DRIVER_STATE_QUERY);

interface CallRecord {
  key: string;
  callId: string;
  tool: string;
  args: Json;
  done: boolean;
  /** Until when the current generation's sandbox waits for this call. */
  holdUntil: number;
  /** Set for calls a driver executes: the id the driver answers with. */
  driverCallId?: string;
  /** The driver step that published this call. */
  step?: number;
}

export async function CodeExecutionWorkflow(
  input: CodeExecutionInput,
): Promise<CodeExecutionResult> {
  const limits: ExecutionLimits = { ...DEFAULT_LIMITS, ...input.limits };
  const executionId = workflowInfo().workflowId;
  const startedAt = Date.now();

  const manifest = new Map<string, ToolManifestEntry>();
  const calls = new Map<string, CallRecord>();
  const journal: JournalEntry[] = [];
  const queue: CallRecord[] = [];
  const hostOutcomes = new Map<string, CallOutcome>();
  const validators = new Map<string, Validator>();

  let generation = 0;
  let generationStartedAt = startedAt;
  let issuedThisGeneration = new Set<string>();
  let priorKeys: string[] = [];
  let diverged: string | undefined;
  let running = 0;
  let rejected = 0;
  const steps: DriverStep[] = [];
  const driverCalls = new Map<string, CallRecord>();
  // Log lines already published through driver steps. Every generation logs
  // the same prefix again, so a step publishes only what follows it.
  let logsReported = 0;
  let finished = false;
  // Bumped whenever calls are registered, to wake `next` waiters.
  let revision = 0;

  const finish = (
    fields: Pick<CodeExecutionResult, "status" | "output" | "error">,
    logs: LogEntry[] = [],
    logsTruncated = false,
  ): CodeExecutionResult => {
    // Fence out any sandbox still attached to this execution.
    generation += 1;
    finished = true;
    return {
      ...fields,
      logs,
      logs_truncated: logsTruncated,
      logs_reported: Math.min(logsReported, logs.length),
      stats: { generations: generation - 1, calls: calls.size, rejected_calls: rejected },
    };
  };
  const fail = (error: ExecutionError, logs?: LogEntry[], truncated?: boolean) =>
    finish({ status: "failed", error }, logs, truncated);

  const validate = (id: string, schema: JsonSchema, value: Json): string | undefined => {
    let validator = validators.get(id);
    if (!validator) {
      validator = new Validator(schema as never, "2020-12", false);
      validators.set(id, validator);
    }
    const result = validator.validate(value);
    if (result.valid) return undefined;
    return result.errors
      .slice(-3)
      .map((error) => `${error.instanceLocation}: ${error.error}`)
      .join("; ");
  };

  if (byteLength(input.source) > limits.max_source_bytes) {
    return fail({ code: "invalid_source", message: "source exceeds max_source_bytes" });
  }
  for (const entry of input.tools) {
    const clash = [...manifest.keys()].find(
      (name) => name.startsWith(`${entry.name}.`) || entry.name.startsWith(`${name}.`),
    );
    const problem = !TOOL_NAME.test(entry.name)
      ? "is not a dotted identifier path"
      : entry.name === SLEEP_TOOL
        ? "is reserved"
        : manifest.has(entry.name)
          ? "is declared twice"
          : clash
            ? `conflicts with ${clash}`
            : undefined;
    if (problem) {
      return fail({ code: "invalid_input", message: `tool ${entry.name} ${problem}` });
    }
    manifest.set(entry.name, entry);
  }

  const record = (call: CallRecord, outcome: CallOutcome): void => {
    call.done = true;
    if (!outcome.ok && isRejection(outcome.error)) rejected += 1;
    journal.push({
      seq: journal.length,
      key: call.key,
      call_id: call.callId,
      tool: call.tool,
      outcome,
      clock: Date.now(),
    });
  };

  const holdUntil = (tool: string): number => {
    const entry = manifest.get(tool);
    // A driver call can only progress once the script is paused.
    const hold = entry?.target.type === "driver" ? 0 : entry?.hold_ms ?? DEFAULT_HOLD_MS;
    return Math.min(Date.now() + hold, generationStartedAt + limits.generation_timeout_ms);
  };

  const checkResult = (call: CallRecord, outcome: CallOutcome): CallOutcome => {
    if (!outcome.ok) return outcome;
    const value = outcome.value === undefined ? null : outcome.value;
    if (byteLength(JSON.stringify(value)) > limits.max_value_bytes) {
      return callError("value_too_large", "result exceeds max_value_bytes");
    }
    const schema = manifest.get(call.tool)?.output_schema;
    if (schema !== undefined) {
      const problem = validate(`out:${call.tool}`, schema, value);
      if (problem) return callError("invalid_result", problem);
    }
    return { ok: true, value };
  };

  const dispatch = async (call: CallRecord, entry: ToolManifestEntry): Promise<CallOutcome> => {
    const request: ToolCallRequest = {
      execution_id: executionId,
      call_id: call.callId,
      tool: call.tool,
      arguments: call.args,
      context: entry.context ?? null,
    };
    const target = entry.target;
    try {
      switch (target.type) {
        case "activity": {
          const activities = proxyActivities<
            Record<string, (request: ToolCallRequest) => Promise<Json>>
          >({
            taskQueue: target.task_queue,
            startToCloseTimeout: target.timeout_ms,
            retry: { maximumAttempts: target.max_attempts ?? 3 },
          });
          const run = activities[target.activity_type];
          if (!run) return callError("target_failed", "activity target has no type");
          return { ok: true, value: await run(request) };
        }
        case "workflow": {
          const value = await executeChild(target.workflow_type, {
            taskQueue: target.task_queue,
            workflowId: call.callId,
            args: [request],
            workflowExecutionTimeout: target.timeout_ms,
            parentClosePolicy: ParentClosePolicy.REQUEST_CANCEL,
          });
          return { ok: true, value: value as Json };
        }
        case "driver":
          return callError("target_failed", "driver calls are not dispatched");
        case "host": {
          await getExternalWorkflowHandle(target.workflow_id).signal(
            CALL_REQUESTED_SIGNAL,
            request,
          );
          const replied = await condition(
            () => hostOutcomes.has(call.callId),
            target.timeout_ms,
          );
          const outcome = hostOutcomes.get(call.callId);
          hostOutcomes.delete(call.callId);
          if (!replied || !outcome) {
            return callError("target_failed", "host did not reply before the call's timeout");
          }
          return outcome;
        }
      }
    } catch (error) {
      return failureOutcome(error);
    }
  };

  const pump = (): void => {
    while (running < limits.max_concurrent_calls) {
      const call = queue.shift();
      if (!call) return;
      const entry = manifest.get(call.tool);
      if (!entry) continue;
      running += 1;
      void dispatch(call, entry).then((outcome) => {
        running -= 1;
        record(call, checkResult(call, outcome));
        pump();
      });
    }
  };

  const register = (issued: IssuedCall): void => {
    const call: CallRecord = {
      key: issued.key,
      callId: `${executionId}/${issued.key}`,
      tool: issued.tool,
      args: issued.arguments,
      done: false,
      holdUntil: holdUntil(issued.tool),
    };
    calls.set(call.key, call);
    issuedThisGeneration.add(call.key);

    const entry = manifest.get(call.tool);
    const schema = call.tool === SLEEP_TOOL ? SLEEP_SCHEMA : entry?.input_schema;
    if (calls.size > limits.max_calls) {
      return record(call, callError("call_limit", "the execution exceeded max_calls"));
    }
    if (schema === undefined) {
      return record(call, callError("unknown_tool", `no tool named ${call.tool}`));
    }
    if (byteLength(JSON.stringify(call.args)) > limits.max_value_bytes) {
      return record(call, callError("value_too_large", "arguments exceed max_value_bytes"));
    }
    const problem = validate(`in:${call.tool}`, schema, call.args);
    if (problem) return record(call, callError("invalid_arguments", problem));

    if (call.tool === SLEEP_TOOL) {
      const ms = (call.args as { ms: number }).ms;
      // Sleeps hold no target capacity, so they bypass the concurrency queue.
      void sleep(ms).then(
        () => record(call, { ok: true, value: null }),
        (error) => record(call, failureOutcome(error)),
      );
      return;
    }
    if (entry?.target.type === "driver") {
      call.driverCallId = `${input.driver_call_prefix ?? "call"}_${calls.size}`;
      driverCalls.set(call.driverCallId, call);
      return;
    }
    queue.push(call);
    pump();
  };

  const requireCurrent = (requested: number): void => {
    if (requested !== generation) {
      throw ApplicationFailure.nonRetryable(
        `generation ${requested} is no longer current`,
        STALE_GENERATION,
      );
    }
  };

  setHandler(issueUpdate, (request): IssueResponse => {
    requireCurrent(request.generation);
    for (const issued of request.calls) {
      const known = calls.get(issued.key);
      if (known) {
        issuedThisGeneration.add(known.key);
        if (!known.done) known.holdUntil = holdUntil(known.tool);
        continue;
      }
      // A deterministic re-run repeats every earlier call before it can make
      // a new one. Refuse to dispatch a call the original run never made.
      const missing = priorKeys.find((key) => !issuedThisGeneration.has(key));
      if (missing !== undefined) {
        diverged =
          `the script called ${issued.tool} before repeating an earlier call ` +
          `to ${calls.get(missing)?.tool ?? missing}`;
        return { status: "diverged", message: diverged };
      }
      register(issued);
    }
    revision += 1;
    return { status: "accepted" };
  });

  setHandler(
    nextUpdate,
    async (request): Promise<NextResponse> => {
      for (;;) {
        requireCurrent(request.generation);
        if (journal.length > request.after) {
          return { status: "available", total: journal.length };
        }
        let latestHold = Number.NEGATIVE_INFINITY;
        for (const call of calls.values()) {
          if (!call.done) latestHold = Math.max(latestHold, call.holdUntil);
        }
        const wait = latestHold - Date.now();
        if (!(wait > 0)) return { status: "suspend" };
        const seen = revision;
        await condition(
          () =>
            journal.length > request.after ||
            generation !== request.generation ||
            revision !== seen,
          wait,
        );
      }
    },
    { unfinishedPolicy: HandlerUnfinishedPolicy.ABANDON },
  );

  setHandler(journalQuery, (request): JournalPage => {
    const entries: JournalEntry[] = [];
    let bytes = 0;
    for (let index = request.after; index < journal.length; index++) {
      const entry = journal[index];
      if (!entry) break;
      bytes += byteLength(JSON.stringify(entry));
      if (entries.length > 0 && bytes > request.max_bytes) break;
      entries.push(entry);
    }
    return { entries, total: journal.length };
  });

  setHandler(driverStateQuery, (): DriverState => {
    const step = steps[steps.length - 1];
    return { status: finished ? "finished" : "running", ...(step ? { step } : {}) };
  });

  setHandler(driverResultsSignal, ({ results }) => {
    for (const { call_id, outcome } of results) {
      const call = driverCalls.get(call_id);
      if (!call || call.done || call.step === undefined) continue;
      record(call, checkResult(call, outcome));
    }
  });

  setHandler(callCompletedSignal, (completed) => {
    const known = [...calls.values()].some(
      (call) => call.callId === completed.call_id && !call.done,
    );
    if (known && !hostOutcomes.has(completed.call_id)) {
      hostOutcomes.set(completed.call_id, completed.outcome);
    }
  });

  const { runScript } = proxyActivities<{
    runScript(input: RunScriptInput): Promise<RunScriptOutput>;
  }>({
    startToCloseTimeout: limits.generation_timeout_ms + 15_000,
    heartbeatTimeout: 20_000,
    // A failed generation is replaced by a new one, never retried in place,
    // so at most one sandbox can speak for a generation number.
    retry: { maximumAttempts: 1 },
  });

  /** Publish the driver calls the script is paused on as a new step. */
  const publishStep = (logs: LogEntry[]): void => {
    const pending = [...driverCalls.values()].filter(
      (call) => !call.done && call.step === undefined,
    );
    if (pending.length === 0) return;
    const index = steps.length;
    for (const call of pending) call.step = index;
    steps.push({
      index,
      calls: pending.map((call) => ({
        call_id: call.driverCallId ?? call.callId,
        tool: call.tool,
        arguments: call.args,
      })),
      logs: logs.slice(logsReported),
    });
    logsReported = Math.max(logsReported, logs.length);
  };

  const seed: [number, number] = [
    Math.floor(Math.random() * 0x1_0000_0000),
    Math.floor(Math.random() * 0x1_0000_0000),
  ];
  const deadlineError: ExecutionError = {
    code: "deadline_exceeded",
    message: "the execution exceeded deadline_ms",
  };
  let sandboxFailures = 0;

  for (;;) {
    if (Date.now() - startedAt >= limits.deadline_ms) return fail(deadlineError);
    if (generation >= limits.max_generations) {
      return fail({
        code: "generation_limit",
        message: "the execution exceeded max_generations",
      });
    }
    generation += 1;
    generationStartedAt = Date.now();
    issuedThisGeneration = new Set();
    priorKeys = [...calls.keys()];

    let output: RunScriptOutput;
    try {
      output = await runScript({
        generation,
        source: input.source,
        input: input.input,
        tool_names: [...manifest.keys()],
        seed,
        start_clock: startedAt,
        limits,
      });
    } catch (error) {
      if (isCancellation(error)) throw error;
      sandboxFailures += 1;
      if (sandboxFailures >= MAX_SANDBOX_FAILURES) {
        return fail({ code: "sandbox_failed", message: failureMessage(error) });
      }
      await sleep(1_000 * sandboxFailures);
      continue;
    }
    sandboxFailures = 0;

    if (diverged !== undefined) return fail({ code: "diverged", message: diverged });
    if (output.status === "failed") {
      return fail(output.error, output.logs, output.logs_truncated);
    }
    if (output.status === "completed") {
      if (input.output_schema !== undefined) {
        const problem = validate("output", input.output_schema, output.output);
        if (problem) {
          return fail(
            { code: "invalid_output", message: problem },
            output.logs,
            output.logs_truncated,
          );
        }
      }
      return finish(
        { status: "completed", output: output.output },
        output.logs,
        output.logs_truncated,
      );
    }

    const delivered = output.delivered;
    publishStep(output.logs);
    const remaining = limits.deadline_ms - (Date.now() - startedAt);
    if (remaining <= 0) return fail(deadlineError);
    const progressed = await condition(() => journal.length > delivered, remaining);
    if (!progressed) return fail(deadlineError);
  }
}

function callError(code: CallError["code"], message: string, details?: Json): CallOutcome {
  return { ok: false, error: details === undefined ? { code, message } : { code, message, details } };
}

function isRejection(error: CallError): boolean {
  return (
    error.code === "unknown_tool" ||
    error.code === "invalid_arguments" ||
    error.code === "call_limit" ||
    (error.code === "value_too_large" && error.message.startsWith("arguments"))
  );
}

function failureOutcome(error: unknown): CallOutcome {
  const cause =
    error instanceof ActivityFailure || error instanceof ChildWorkflowFailure
      ? error.cause
      : error;
  if (cause instanceof ApplicationFailure && cause.type === TOOL_ERROR_TYPE) {
    const details = cause.details?.[0] as Json | undefined;
    return callError("tool_error", cause.message, details);
  }
  return callError("target_failed", failureMessage(cause ?? error));
}

function failureMessage(error: unknown): string {
  if (error instanceof ActivityFailure || error instanceof ChildWorkflowFailure) {
    return error.cause?.message ?? error.message;
  }
  return error instanceof Error ? error.message : String(error);
}
