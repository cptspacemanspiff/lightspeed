import { fileURLToPath } from "node:url";
import { ApplicationFailure } from "@temporalio/common";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  CODE_EXECUTION_WORKFLOW,
  DRIVER_RESULTS_SIGNAL,
  DRIVER_STATE_QUERY,
  TOOL_ERROR_TYPE,
  type CodeExecutionInput,
  type CodeExecutionResult,
  type DriverResults,
  type DriverState,
  type DriverStep,
  type Json,
  type ToolCallRequest,
  type ToolManifestEntry,
} from "../src/contract.js";
import { QuickJsSandbox } from "../src/sandbox/quickjs.js";
import type { Sandbox, SandboxOptions, SandboxSession } from "../src/sandbox/types.js";
import { createCodeRuntimeWorker } from "../src/worker.js";

const TOOLS_QUEUE = "test-tools";

/** Fails the first sandbox after it has been handed one result. */
class CrashingSandbox implements Sandbox {
  crashed = false;
  private readonly inner = new QuickJsSandbox();

  async start(options: SandboxOptions): Promise<SandboxSession> {
    const session = await this.inner.start(options);
    if (this.crashed) return session;
    return {
      run: () => session.run(),
      deliver: () => {
        this.crashed = true;
        throw new Error("sandbox crashed");
      },
      logs: () => session.logs(),
      dispose: () => session.dispose(),
    };
  }
}

/** Gives every generation a different seed, as a broken runtime would. */
class SkewedSandbox implements Sandbox {
  private readonly inner = new QuickJsSandbox();
  private starts = 0;

  start(options: SandboxOptions): Promise<SandboxSession> {
    this.starts += 1;
    return this.inner.start({ ...options, seed: [this.starts, this.starts] });
  }
}

let env: TestWorkflowEnvironment;
const workers: Worker[] = [];
let running: Promise<unknown>;
const crashing = new CrashingSandbox();

const invocations = new Map<string, number>();
let active = 0;
let peak = 0;

const count = (request: ToolCallRequest): void => {
  invocations.set(request.tool, (invocations.get(request.tool) ?? 0) + 1);
};

const toolActivities = {
  async echo(request: ToolCallRequest): Promise<Json> {
    count(request);
    return { echo: request.arguments, context: request.context };
  },
  async fail(request: ToolCallRequest): Promise<Json> {
    count(request);
    throw ApplicationFailure.nonRetryable("no such page", TOOL_ERROR_TYPE, { status: 404 });
  },
  async tracked(request: ToolCallRequest): Promise<Json> {
    count(request);
    active += 1;
    peak = Math.max(peak, active);
    await new Promise((resolve) => setTimeout(resolve, 150));
    active -= 1;
    return null;
  },
};

const activityTool = (name: string, extra: Partial<ToolManifestEntry> = {}): ToolManifestEntry => ({
  name,
  input_schema: { type: "object" },
  target: {
    type: "activity",
    task_queue: TOOLS_QUEUE,
    activity_type: name.split(".").pop() ?? name,
    timeout_ms: 10_000,
    max_attempts: 1,
  },
  ...extra,
});

const slowTool: ToolManifestEntry = {
  name: "slow.double",
  input_schema: {
    type: "object",
    properties: { n: { type: "number" } },
    required: ["n"],
  },
  output_schema: {
    type: "object",
    properties: { doubled: { type: "number" } },
    required: ["doubled"],
  },
  target: { type: "workflow", task_queue: TOOLS_QUEUE, workflow_type: "slowDouble", timeout_ms: 30_000 },
  // Never wait in the sandbox: every call to this tool suspends the script.
  hold_ms: 0,
};

type TestInput = Omit<CodeExecutionInput, "input" | "tools"> & Partial<CodeExecutionInput>;

let executions = 0;
let lastWorkflowId = "";

async function execute(input: TestInput, taskQueue = "code-runtime"): Promise<CodeExecutionResult> {
  lastWorkflowId = `code-test-${executions++}-${Date.now()}`;
  return env.client.workflow.execute<(input: CodeExecutionInput) => Promise<CodeExecutionResult>>(
    CODE_EXECUTION_WORKFLOW,
    { taskQueue, workflowId: lastWorkflowId, args: [{ input: null, tools: [], ...input }] },
  );
}

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createLocal();
  const { nativeConnection: connection, client } = env;
  workers.push(
    await createCodeRuntimeWorker({ connection, client }),
    await createCodeRuntimeWorker({ connection, client, taskQueue: "code-runtime-crash", sandbox: crashing }),
    await createCodeRuntimeWorker({
      connection,
      client,
      taskQueue: "code-runtime-skew",
      sandbox: new SkewedSandbox(),
    }),
    await Worker.create({
      connection,
      taskQueue: TOOLS_QUEUE,
      workflowsPath: fileURLToPath(new URL("./fixtures/workflows.ts", import.meta.url)),
      activities: toolActivities,
    }),
  );
  running = Promise.all(workers.map((worker) => worker.run()));
});

afterAll(async () => {
  for (const worker of workers) worker.shutdown();
  await running;
  await env?.teardown();
});

beforeEach(() => {
  invocations.clear();
  active = 0;
  peak = 0;
});

describe("CodeExecutionWorkflow", () => {
  it("runs a script with no calls in one generation", async () => {
    const result = await execute({
      source: `console.log("hello"); return { sum: (input.values as number[]).reduce((a, b) => a + b, 0) };`,
      input: { values: [1, 2, 3] },
      output_schema: { type: "object", properties: { sum: { const: 6 } }, required: ["sum"] },
    });
    expect(result).toEqual({
      status: "completed",
      output: { sum: 6 },
      logs: [{ level: "info", text: "hello" }],
      logs_truncated: false,
      logs_reported: 0,
      stats: { generations: 1, calls: 0, rejected_calls: 0 },
    });
  });

  it("dispatches concurrent calls to activity targets with their context", async () => {
    const result = await execute({
      source: `return Promise.all([1, 2, 3].map((n) => tools.util.echo({ n })));`,
      tools: [activityTool("util.echo", { context: { tenant: "acorn" } })],
    });
    expect(result.status).toBe("completed");
    expect(result.output).toEqual(
      [1, 2, 3].map((n) => ({ echo: { n }, context: { tenant: "acorn" } })),
    );
    expect(invocations.get("util.echo")).toBe(3);
    expect(result.stats).toEqual({ generations: 1, calls: 3, rejected_calls: 0 });
  });

  it("suspends on a slow call and re-runs against the journal without repeating calls", async () => {
    const result = await execute({
      source: `
        const before = await tools.util.echo({ step: "before" });
        // Anything nondeterministic here would change this call's identity
        // on the re-run and be reported as divergence.
        const first = await tools.slow.double({
          n: 4, r: Math.random(), id: crypto.randomUUID(), at: Date.now(),
        });
        const [second, third] = await Promise.all([
          tools.slow.double({ n: first.doubled }),
          tools.slow.double({ n: 1, delay_ms: 200 }),
        ]);
        const after = await tools.util.echo({ step: "after" });
        return { before, first, second, third, after };`,
      tools: [activityTool("util.echo"), slowTool],
    });
    expect(result.status).toBe("completed");
    expect(result.output).toMatchObject({
      first: { doubled: 8 },
      second: { doubled: 16 },
      third: { doubled: 2 },
      after: { echo: { step: "after" } },
    });
    expect(result.stats.generations).toBeGreaterThanOrEqual(3);
    expect(result.stats.calls).toBe(5);
    expect(invocations.get("util.echo")).toBe(2);

    // The workflow itself must replay: updates, timers, children, and the
    // seed all come back from history.
    const history = await env.client.workflow.getHandle(lastWorkflowId).fetchHistory();
    await Worker.runReplayHistory(
      { workflowsPath: fileURLToPath(new URL("../src/workflows.ts", import.meta.url)) },
      history,
      // Call ids derive from the execution's workflow id.
      lastWorkflowId,
    );
  });

  it("recovers from a sandbox that dies mid-run", async () => {
    crashing.crashed = false;
    const result = await execute(
      {
        source: `
          const a = await tools.util.echo({ n: 1 });
          const b = await tools.util.echo({ n: 2 });
          return [a.echo.n, b.echo.n];`,
        tools: [activityTool("util.echo")],
      },
      "code-runtime-crash",
    );
    expect(result).toMatchObject({ status: "completed", output: [1, 2] });
    expect(result.stats.generations).toBe(2);
    expect(invocations.get("util.echo")).toBe(2);
  });

  it("fails rather than dispatch calls from a re-run that diverged", async () => {
    const result = await execute(
      {
        source: `
          await tools.slow.double({ n: Math.random() });
          return "unreachable";`,
        tools: [slowTool],
      },
      "code-runtime-skew",
    );
    expect(result).toMatchObject({ status: "failed", error: { code: "diverged" } });
    expect(result.stats.calls).toBe(1);
  });

  it("returns rejected and failed calls to the script as ToolErrors", async () => {
    const result = await execute({
      source: `
        const codes = [];
        for (const attempt of [
          () => tools.slow.double({ n: "four" }),
          () => tools.web.fail({}),
          () => tools.strict.echo({}),
        ]) {
          try { await attempt(); codes.push("ok"); }
          catch (error) { codes.push([error.code, error.details ?? null]); }
        }
        return codes;`,
      tools: [
        slowTool,
        activityTool("web.fail"),
        activityTool("strict.echo", { output_schema: { type: "string" } }),
      ],
    });
    expect(result.status).toBe("completed");
    expect(result.output).toEqual([
      ["invalid_arguments", null],
      ["tool_error", { status: 404 }],
      ["invalid_result", null],
    ]);
    expect(result.stats).toMatchObject({ calls: 3, rejected_calls: 1 });
  });

  it("limits total and concurrent calls", async () => {
    const result = await execute({
      source: `
        const settled = await Promise.allSettled(
          [1, 2, 3, 4, 5, 6, 7].map((n) => tools.util.tracked({ n })),
        );
        return settled.map((entry) => entry.status === "fulfilled" ? "ok" : entry.reason.code);`,
      tools: [activityTool("util.tracked")],
      limits: { max_calls: 6, max_concurrent_calls: 2 },
    });
    expect(result.output).toEqual(["ok", "ok", "ok", "ok", "ok", "ok", "call_limit"]);
    expect(invocations.get("util.tracked")).toBe(6);
    expect(peak).toBe(2);
  });

  it("routes host targets by signal and sleeps on a workflow timer", async () => {
    const host = await env.client.workflow.start("toolHost", {
      taskQueue: TOOLS_QUEUE,
      workflowId: `tool-host-${Date.now()}`,
    });
    const result = await execute({
      source: `
        const started = Date.now();
        await sleep(300);
        const reply = await tools.session.lookup({ id: 7 });
        return { reply, waited: Date.now() - started >= 300 };`,
      tools: [
        {
          name: "session.lookup",
          input_schema: { type: "object" },
          target: { type: "host", workflow_id: host.workflowId, timeout_ms: 10_000 },
          context: "session-1",
        },
      ],
    });
    await host.signal("stop");
    expect(result).toMatchObject({
      status: "completed",
      output: { reply: { handled: "session.lookup", by: "session-1" }, waited: true },
    });
  });

  it("publishes driver calls as steps and continues with the driver's results", async () => {
    const workflowId = `code-driver-${Date.now()}`;
    const handle = await env.client.workflow.start(CODE_EXECUTION_WORKFLOW, {
      taskQueue: "code-runtime",
      workflowId,
      args: [
        {
          source: `
            console.log("starting");
            const [a, b] = await Promise.all([tools.add({ a: 1, b: 2 }), tools.add({ a: 3, b: 4 })]);
            console.log("sum", a + b);
            const doubled = await tools.add({ a: a + b, b: a + b });
            return { doubled };`,
          input: null,
          driver_call_prefix: "r1",
          tools: [
            {
              name: "add",
              input_schema: {
                type: "object",
                properties: { a: { type: "number" }, b: { type: "number" } },
                required: ["a", "b"],
              },
              target: { type: "driver" },
            },
          ],
        } satisfies CodeExecutionInput,
      ],
    });
    const nextStep = async (index: number): Promise<DriverStep> => {
      for (;;) {
        const state = await handle.query<DriverState>(DRIVER_STATE_QUERY);
        if (state.step && state.step.index === index) return state.step;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    };
    const answer = async (step: DriverStep) =>
      handle.signal(DRIVER_RESULTS_SIGNAL, {
        results: step.calls.map((call) => {
          const { a, b } = call.arguments as { a: number; b: number };
          return { call_id: call.call_id, outcome: { ok: true, value: a + b } };
        }),
      } satisfies DriverResults);

    const first = await nextStep(0);
    expect(first.calls.map((call) => call.call_id)).toEqual(["r1_1", "r1_2"]);
    expect(first.logs).toEqual([{ level: "info", text: "starting" }]);
    await answer(first);
    const second = await nextStep(1);
    expect(second.calls).toEqual([{ call_id: "r1_3", tool: "add", arguments: { a: 10, b: 10 } }]);
    // Only what the script logged since the previous step.
    expect(second.logs).toEqual([{ level: "info", text: "sum 10" }]);
    await answer(second);

    const result = (await handle.result()) as CodeExecutionResult;
    expect(result).toMatchObject({ status: "completed", output: { doubled: 20 }, logs_reported: 2 });
    expect(result.logs.slice(result.logs_reported)).toEqual([]);
  });

  it.each<[string, string, Partial<TestInput>, string]>([
    ["a thrown error", `throw new Error("boom");`, {}, "script_error"],
    ["a syntax error", `return (;`, {}, "invalid_source"],
    ["a result that fails the output schema", `return 1;`, { output_schema: { type: "string" } }, "invalid_output"],
    ["a wait on something that is not a call", `await new Promise(() => {});`, {}, "stalled"],
    ["a script that never yields", `for (;;) {}`, { limits: { generation_timeout_ms: 300 } }, "generation_timeout"],
    ["an invalid manifest", `return 1;`, { tools: [activityTool("sleep")] }, "invalid_input"],
    [
      "a script that exhausts its memory",
      `const hoard = []; for (;;) hoard.push(new Array(100_000).fill(hoard.length));`,
      { limits: { sandbox_memory_bytes: 32 * 1024 * 1024 } },
      "sandbox_failed",
    ],
  ])("fails on %s", async (_name, source, extra, code) => {
    const result = await execute({ source, ...extra });
    expect(result).toMatchObject({ status: "failed", error: { code } });
  });
});
