import { beforeEach, describe, expect, it } from "vitest";
import type { CallOutcome, Json } from "../src/contract.js";
import { generateDeclarations } from "../src/declarations.js";
import { QuickJsSandbox } from "../src/sandbox/quickjs.js";
import type { Sandbox, SandboxProgress, SandboxSession } from "../src/sandbox/types.js";
import { WorkerdSandbox } from "../src/sandbox/workerd/sandbox.js";
import { compileSource } from "../src/source.js";

let sandbox: Sandbox;

async function start(
  body: string,
  overrides: { input?: Json; tools?: string[]; seed?: [number, number]; deadlineMs?: number } = {},
): Promise<SandboxSession> {
  const compiled = compileSource(body);
  if (!compiled.ok) throw new Error(compiled.message);
  return sandbox.start({
    code: compiled.code,
    input: overrides.input ?? null,
    toolNames: overrides.tools ?? ["web.fetch", "math.add"],
    seed: overrides.seed ?? [1, 2],
    startClock: 1_000,
    memoryBytes: 32 * 1024 * 1024,
    maxLogBytes: 1024,
    deadline: Date.now() + (overrides.deadlineMs ?? 5_000),
  });
}

/** Answer every call with `respond` until the script settles. */
async function drive(
  session: SandboxSession,
  respond: (tool: string, args: Json) => CallOutcome = () => ({ ok: true, value: null }),
): Promise<Exclude<SandboxProgress, { status: "pending" }>> {
  let clock = 1_000;
  try {
    for (;;) {
      const progress = await session.run();
      if (progress.status !== "pending") return progress;
      if (progress.issued.length === 0) throw new Error("script stalled");
      for (const call of progress.issued) {
        clock += 10;
        session.deliver(call.id, respond(call.tool, JSON.parse(call.argumentsJson) as Json), clock);
      }
    }
  } finally {
    session.dispose();
  }
}

async function output(body: string, overrides: Parameters<typeof start>[1] = {}): Promise<Json> {
  const progress = await drive(await start(body, overrides));
  if (progress.status !== "completed") throw new Error(JSON.stringify(progress));
  return JSON.parse(progress.outputJson) as Json;
}

describe.each<[string, () => Sandbox]>([
  ["QuickJsSandbox", () => new QuickJsSandbox()],
  ["WorkerdSandbox", () => new WorkerdSandbox()],
])("%s", (_name, create) => {
  beforeEach(() => {
    sandbox = create();
  });

  it("issues concurrent calls before it blocks and returns the script's value", async () => {
    const session = await start(
      `const [a, b] = await Promise.all([
         tools.math.add({ a: 1, b: 2 }),
         tools.math.add({ a: 3, b: 4 }),
       ]);
       return { total: a + b, greeting: input.name as string };`,
      { input: { name: "acorn" } },
    );
    const first = await session.run();
    expect(first.status).toBe("pending");
    expect(first.status === "pending" && first.issued.map((call) => call.tool)).toEqual([
      "math.add",
      "math.add",
    ]);
    if (first.status !== "pending") throw new Error("expected pending");
    // Deliver out of issue order: the script still pairs results with calls.
    const [early, late] = first.issued;
    if (!early || !late) throw new Error("expected two calls");
    session.deliver(late.id, { ok: true, value: 7 }, 1_000);
    expect(await session.run()).toEqual({ status: "pending", issued: [] });
    session.deliver(early.id, { ok: true, value: 3 }, 1_000);
    expect(await session.run()).toEqual({
      status: "completed",
      issued: [],
      outputJson: JSON.stringify({ total: 10, greeting: "acorn" }),
    });
    session.dispose();
  });

  it("runs a whole script end to end", async () => {
    const session = await start(
      `const sum = await tools.math.add({ a: 1, b: 2 });
       const page = await tools.web.fetch({ url: "https://example.test/" + sum });
       return { sum, page };`,
    );
    const progress = await drive(session, (tool, args) =>
      tool === "math.add"
        ? { ok: true, value: 3 }
        : { ok: true, value: (args as { url: string }).url },
    );
    expect(progress).toMatchObject({
      status: "completed",
      outputJson: JSON.stringify({ sum: 3, page: "https://example.test/3" }),
    });
  });

  it("derives randomness and time from the seed and the delivered clock", async () => {
    const body = `
      const before = [Math.random(), crypto.randomUUID(), Date.now(), new Date().getTime()];
      await tools.math.add({ a: 1, b: 1 });
      return { before, after: [Math.random(), Date.now(), performance.now()] };`;
    const first = await output(body);
    expect(await output(body)).toEqual(first);
    expect(await output(body, { seed: [7, 7] })).not.toEqual(first);
    const { before, after } = first as { before: Json[]; after: Json[] };
    expect(before.slice(2)).toEqual([1_000, 1_000]);
    expect(after.slice(1)).toEqual([1_010, 10]);
  });

  it("exposes nothing but the script surface", async () => {
    expect(
      await output(`return [
        typeof __hostCall, typeof __hostDone, typeof __start, typeof setTimeout,
        typeof WeakRef, typeof FinalizationRegistry, typeof fetch, typeof require,
        Object.isFrozen(tools), new Date(0).constructor === Date,
      ];`),
    ).toEqual([
      "undefined", "undefined", "undefined", "undefined",
      "undefined", "undefined", "undefined", "undefined",
      true, true,
    ]);
  });

  it("carries values at the default size limit in both directions", async () => {
    const size = 500 * 1024;
    const session = await start(
      `const page = await tools.web.fetch({ body: "a".repeat(${size}) });
       return page.length;`,
    );
    const progress = await drive(session, (_tool, args) => ({
      ok: true,
      value: (args as { body: string }).body.replaceAll("a", "b"),
    }));
    expect(progress).toMatchObject({ status: "completed", outputJson: String(size) });
  });

  it("raises a failed call as a catchable ToolError", async () => {
    const session = await start(
      `try {
         await tools.web.fetch({ url: "x" });
       } catch (error) {
         return [error instanceof ToolError, error.tool, error.code, error.message];
       }`,
    );
    const progress = await drive(session, () => ({
      ok: false,
      error: { code: "tool_error", message: "unreachable" },
    }));
    expect(progress.status === "completed" && JSON.parse(progress.outputJson)).toEqual([
      true,
      "web.fetch",
      "tool_error",
      "web.fetch: unreachable",
    ]);
  });

  it("reports a thrown error with the script's message", async () => {
    const progress = await drive(await start(`throw new RangeError("out of range");`));
    expect(progress).toMatchObject({
      status: "failed",
      error: { code: "script_error", message: "RangeError: out of range" },
    });
  });

  it("interrupts a script that never yields", async () => {
    const progress = await drive(await start(`for (;;) {}`, { deadlineMs: 200 }));
    expect(progress).toMatchObject({ status: "failed", error: { code: "generation_timeout" } });
  });

  it("caps captured logs", async () => {
    const session = await start(
      `console.log("kept", { n: 1 }); console.warn("x".repeat(2000)); console.log("dropped");`,
    );
    await session.run();
    expect(session.logs()).toEqual({
      entries: [{ level: "info", text: 'kept {"n":1}' }],
      truncated: true,
    });
    session.dispose();
  });
});

describe("compileSource", () => {
  it("strips types", () => {
    const compiled = compileSource(`const n: number = 1; return n as number;`);
    expect(compiled.ok && compiled.code).not.toContain(": number");
  });

  it("rejects a syntax error with its position in the body", () => {
    expect(compileSource(`const ok = 1;\nconst = 2;`)).toMatchObject({
      ok: false,
      message: expect.stringMatching(/^line 2, column \d+: /),
    });
  });

  it("rejects a body that closes the wrapper", () => {
    expect(compileSource(`return 1; } globalThis.leak = 1; async function other() {`)).toEqual({
      ok: false,
      message: "the script must be a single async function body",
    });
  });
});

describe("generateDeclarations", () => {
  it("renders the manifest as a typed namespace", () => {
    const declarations = generateDeclarations(
      [
        {
          name: "web.fetch",
          description: "Fetch a page.",
          input_schema: {
            type: "object",
            properties: { url: { type: "string", description: "Absolute URL." } },
            required: ["url"],
          },
          output_schema: {
            type: "object",
            properties: {
              status: { type: "integer" },
              body: { type: ["string", "null"] },
              tags: { type: "array", items: { enum: ["a", "b"] } },
            },
            required: ["status"],
          },
        },
        { name: "notify", input_schema: { type: "object", additionalProperties: false } },
      ],
      { inputSchema: { type: "object", properties: { urls: { type: "array", items: { type: "string" } } } } },
    );
    expect(declarations).toContain("declare const input: {\n  urls?: string[];\n};");
    expect(declarations).toContain(
      [
        "declare const tools: {",
        "  web: {",
        "    /** Fetch a page. */",
        "    fetch(args: {",
        "      /** Absolute URL. */",
        "      url: string;",
        "    }): Promise<{",
        "      status: number;",
        "      body?: string | null;",
        '      tags?: Array<"a" | "b">;',
        "    }>;",
        "  };",
        "  notify(args: {}): Promise<unknown>;",
        "};",
      ].join("\n"),
    );
  });
});
