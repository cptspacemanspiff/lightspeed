import { Hono } from "hono";
import { afterEach, expect, it, vi } from "vitest";
import type { ApiVariables, AppContext } from "../context.js";
import { gatewayRoutes } from "./gateway.js";

vi.mock("./universes.js", () => ({ universeForSession: vi.fn(async (_ctx, _c, id: string) => ({
  universe: { lightspeedUniverseId: id, gatewayUrl: "https://engine.example/rpc" },
  slug: "test", role: "contributor", member: { userId: "member", role: "contributor" },
})) }));
afterEach(() => vi.unstubAllGlobals());

const counts = { total: 1, pending: 0, waiting: 0, succeeded: 1, failed: 0, cancelled: 0 };

function fixture() {
  const requests: { method: string; params: Record<string, unknown> }[] = [];
  vi.stubGlobal("fetch", vi.fn(async (_url: unknown, init: RequestInit) => {
    const rpc = JSON.parse(String(init.body));
    // The member gate reads the session before a session-targeted call.
    if (rpc.method === "session/read") {
      return Response.json({ id: rpc.id, result: { result: { session: { access: { visibility: "universe" } } }, notifications: [] } });
    }
    requests.push(rpc);
    const result = rpc.method === "session/runs/list"
      ? { runs: [{ id: "run_2", status: "completed" }], nextCursor: "run_2", hasOlderRuns: true }
      : rpc.method === "session/runs/calls/list"
        ? { calls: [{ id: "code-tool:a", requestId: "call-1" }], nextCursor: "7", counts }
        : { run: { id: "run_3", status: "running" } };
    return Response.json({ id: rpc.id, result: { result, notifications: [] } });
  }));
  const app = new Hono<{ Variables: ApiVariables }>();
  app.use("*", async (c, next) => { c.set("session", { user: { id: "member" } } as ApiVariables["session"]); await next(); });
  app.route("/", gatewayRoutes({ env: { lightspeedApiUrl: "https://engine.example/rpc", lightspeedApiKey: "lsk_fixture" } } as AppContext));
  return { app, requests };
}

it("starts a program run from JSON input", async () => {
  const f = fixture();
  const response = await f.app.request("/universe/sessions/s1/program-runs", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ input: { claim: "sky", n: 2 }, submissionId: "sub" }),
  });
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({ run: { id: "run_3", status: "running" } });
  expect(f.requests[0]).toMatchObject({
    method: "session/runs/start",
    params: { sessionId: "s1", submissionId: "sub", source: { type: "program", input: { claim: "sky", n: 2 } } },
  });
});

it("refuses a program run without a submission id", async () => {
  const f = fixture();
  const response = await f.app.request("/universe/sessions/s1/program-runs", {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ input: {} }),
  });
  expect(response.status).toBe(400);
  expect(f.requests).toEqual([]);
});

it("pages runs and a run's calls", async () => {
  const f = fixture();
  const runs = await f.app.request("/universe/sessions/s1/runs?limit=5&cursor=run_9");
  expect(await runs.json()).toEqual({ runs: [{ id: "run_2", status: "completed" }], nextCursor: "run_2" });
  expect(f.requests[0]).toMatchObject({
    method: "session/runs/list", params: { sessionId: "s1", limit: 5, cursor: "run_9" },
  });
  const calls = await f.app.request("/universe/sessions/s1/runs/run_2/calls?cursor=3&status=failed&limit=999");
  expect(await calls.json()).toEqual({ calls: [{ id: "code-tool:a", requestId: "call-1" }], nextCursor: "7", counts });
  expect(f.requests[1]).toMatchObject({
    method: "session/runs/calls/list",
    params: { sessionId: "s1", runId: "run_2", cursor: "3", status: "failed", limit: 200 },
  });
  // An unknown status filter is dropped rather than sent.
  await f.app.request("/universe/sessions/s1/runs/run_2/calls?status=bogus");
  expect(f.requests[2]!.params).not.toHaveProperty("status");
});
