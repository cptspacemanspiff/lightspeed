// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { ProgramCallView, RunSummaryView } from "@lightspeed-ai/sdk";
import { waitForUi } from "@/test/wait-for-ui";
import { ProgramPanel } from "./program-panel";

const mocks = vi.hoisted(() => ({ api: vi.fn() }));
vi.mock("@/api", async (original) => ({ ...await original<typeof import("@/api")>(), api: mocks.api }));

const base = "/api/v1/universes/universe/sessions/program";
const completed: RunSummaryView = {
  id: "run_1",
  status: "completed",
  acceptedAtMs: 1_000,
  startedAtMs: 1_000,
  completedAtMs: 3_500,
  source: { type: "program", inputRef: "sha256:input" },
  output: { contentRef: "sha256:output", mediaType: "application/json" },
};
const failed: RunSummaryView = {
  id: "run_2",
  status: "failed",
  acceptedAtMs: 4_000,
  startedAtMs: 4_000,
  completedAtMs: 4_100,
  source: { type: "program", inputRef: "sha256:input" },
  failure: { kind: "program", message: "boom" },
};
const preview = (text: string) => ({ contentRef: `sha256:${text.length}`, preview: text });
const calls: ProgramCallView[] = [
  {
    id: "code-tool:a",
    execution: 0,
    requestId: "call-1",
    contract: "agent.run",
    tool: "agent_run",
    annotation: "the claim cites it",
    status: "succeeded",
    arguments: preview('{"agent":"checker","input":"CHECK sky"}'),
    output: preview('{"status":"completed","output":"checked sky"}'),
    admittedAtMs: 1_100,
    completedAtMs: 2_300,
    childSessionId: "agent_child",
  },
  {
    id: "code-tool:b",
    execution: 1,
    requestId: "call-1",
    contract: "input.request",
    tool: "input_request",
    status: "pending",
    arguments: preview('{"ask":"Ship?"}'),
    admittedAtMs: 2_400,
    inputRequestId: "inp_9",
  },
];

let root: Root;
let container: HTMLDivElement;
let client: QueryClient;
let posted: unknown[];
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  posted = [];
  mocks.api.mockReset().mockImplementation(async (method: string, path: string, body?: unknown) => {
    if (method === "GET" && path === `${base}/runs?limit=20`) return { runs: [failed, completed], nextCursor: null };
    if (method === "GET" && path.startsWith(`${base}/runs/run_1/calls?`)) {
      return { calls, nextCursor: null, counts: { total: 2, pending: 1, waiting: 0, succeeded: 1, failed: 0, cancelled: 0 } };
    }
    if (method === "GET" && path.startsWith(`${base}/runs/run_1/notes?`)) {
      return { notes: [], nextCursor: null };
    }
    if (method === "GET" && path.startsWith(`${base}/runs/run_2/notes?`)) {
      return {
        notes: [
          { execution: 0, seq: 1, kind: "said", text: "checking one claim", afterCalls: 0, recordedAtMs: 4_000 },
          { execution: 0, seq: 2, kind: "stepStarted", text: "check sky", afterCalls: 0, recordedAtMs: 4_000 },
          { execution: 0, seq: 3, kind: "said", step: 2, text: "fetched it", afterCalls: 1, recordedAtMs: 4_050 },
          { execution: 0, seq: 4, kind: "stepEnded", step: 2, afterCalls: 1, recordedAtMs: 4_080 },
        ],
        nextCursor: null,
      };
    }
    if (method === "GET" && path.startsWith(`${base}/runs/run_2/calls?`) && path.includes("status=")) {
      return { calls: [], nextCursor: null, counts: { total: 0, pending: 0, waiting: 0, succeeded: 0, failed: 0, cancelled: 0 } };
    }
    if (method === "GET" && path.startsWith(`${base}/runs/run_2/calls?`)) {
      return {
        calls: [{ ...calls[0]!, id: "code-tool:s", step: 2, childSessionId: undefined }],
        nextCursor: null,
        counts: { total: 1, pending: 0, waiting: 0, succeeded: 1, failed: 0, cancelled: 0 },
      };
    }
    if (method === "POST" && path === `${base}/program-runs`) {
      posted.push(body);
      return { run: { id: "run_3", status: "running" } };
    }
    throw new Error(`Unexpected request: ${method} ${path}`);
  });
  client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: Infinity } } });
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  client.clear();
  container.remove();
  vi.unstubAllGlobals();
});

async function render(canControl = true) {
  const loadFullText = vi.fn(async (blobRef: string) =>
    blobRef === "sha256:output" ? '{"verdict":"supported"}' : '{"claim":"sky"}');
  await act(async () => {
    root.render(
      <QueryClientProvider client={client}>
        <MemoryRouter>
          <ProgramPanel
            universeId="universe"
            slug="test"
            sessionId="program"
            driver={{ kind: "program", programId: "checker", revision: 3, semanticsVersion: 1 }}
            runRevision={0}
            canControl={canControl}
            closed={false}
            sessionHref={(id) => `/u/test/sessions/${id}`}
            loadFullText={loadFullText}
          />
        </MemoryRouter>
      </QueryClientProvider>,
    );
  });
  return loadFullText;
}

function click(element: Element | null | undefined) {
  if (!element) throw new Error("missing element");
  (element as HTMLElement).click();
}

it("lists runs newest first with failures, and opens a run's output and calls", async () => {
  await render();
  await waitForUi(() => expect(container.querySelectorAll("[data-run]")).toHaveLength(2));
  expect(container.textContent).toContain("checker");
  expect(container.textContent).toContain("revision 3");
  const [newest, older] = container.querySelectorAll("[data-run]");
  expect(newest!.textContent).toContain("Failed");
  expect(newest!.textContent).toContain("boom");

  await act(async () => click(older!.querySelector("button[aria-expanded]")));
  await waitForUi(() => expect(older!.textContent).toContain('"verdict": "supported"'));
  await waitForUi(() => expect(older!.querySelectorAll("[data-call]")).toHaveLength(2));
  expect(older!.textContent).toContain("2 total · 1 succeeded · 1 pending");
  const links = [...older!.querySelectorAll("a")].map((link) => [link.textContent, link.getAttribute("href")]);
  expect(links).toEqual([
    ["Child session", "/u/test/sessions/agent_child"],
    ["Inbox request", "/u/test/inbox/inp_9"],
  ]);
  // A later execution's call is numbered by its execution; a call shows why
  // it was made in place of its arguments.
  expect(older!.textContent).toContain("#2 call-1");
  const [annotated, plain] = older!.querySelectorAll("[data-call]");
  expect(annotated!.textContent).toContain("the claim cites it");
  expect(annotated!.textContent).not.toContain("CHECK sky");
  expect(plain!.textContent).toContain('{"ask":"Ship?"}');
});

it("shows a run's steps with their narration and calls", async () => {
  await render();
  await waitForUi(() => expect(container.querySelectorAll("[data-run]")).toHaveLength(2));
  const newest = container.querySelectorAll("[data-run]")[0]!;
  await waitForUi(() => expect(newest.querySelector("[data-step]")).not.toBeNull());
  const step = newest.querySelector("[data-step]")!;
  expect(step.getAttribute("data-step")).toBe("check sky");
  expect(step.textContent).toContain("80ms");
  expect(step.textContent).toContain("2 items");
  expect(step.textContent).toContain("fetched it");
  expect(step.querySelectorAll("[data-call]")).toHaveLength(1);
  // Narration outside every step sits above it.
  expect(newest.querySelector("[data-note='1']")!.textContent).toBe("checking one claim");
  // Filtering the calls leaves the plain list.
  const failed = [...newest.querySelectorAll("button")].find((button) => button.textContent === "Failed")!;
  await act(async () => click(failed));
  await waitForUi(() => expect(newest.querySelector("[data-step]")).toBeNull());
});

it("starts a run from JSON input and refuses text that is not JSON", async () => {
  await render();
  await waitForUi(() => expect(container.querySelector("textarea")).not.toBeNull());
  const textarea = container.querySelector("textarea")!;
  const type = async (text: string) => {
    await act(async () => {
      const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!;
      setter.call(textarea, text);
      textarea.dispatchEvent(new Event("input", { bubbles: true }));
    });
  };
  const submit = () => act(async () => click(container.querySelector("button[type=submit]")));

  await type("{claim: sky}");
  await submit();
  expect(container.querySelector("[role=alert]")!.textContent).toContain("Input is not JSON");
  expect(posted).toEqual([]);

  await type('{"claim":"sky"}');
  await submit();
  await waitForUi(() => expect(posted).toHaveLength(1));
  expect(posted[0]).toMatchObject({ input: { claim: "sky" }, submissionId: expect.any(String) });
});

it("offers no run form to a reader", async () => {
  await render(false);
  await waitForUi(() => expect(container.querySelectorAll("[data-run]")).toHaveLength(2));
  expect(container.querySelector("textarea")).toBeNull();
});
