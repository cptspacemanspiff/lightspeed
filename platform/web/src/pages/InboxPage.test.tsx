// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { InputRequestView } from "@/api";
import { PermissionIdentityProvider } from "@/lib/permissions";
import { InboxPage, deadlineLabel, parseAnswerValue } from "./InboxPage";

const mocks = vi.hoisted(() => ({ api: vi.fn(), role: "contributor" }));
vi.mock("@/api", async (original) => ({ ...await original<typeof import("@/api")>(), api: mocks.api }));
vi.mock("@/lib/universes", () => ({
  useActiveUniverse: () => ({ universe: { id: "universe", slug: "test", name: "Test", role: mocks.role }, slug: "test", isLoading: false }),
}));

const NOW = 1_800_000_000_000;
const pending: InputRequestView = {
  requestId: "inp_1",
  sessionId: "review-session",
  runId: 3,
  ask: "Merge pr-7?",
  payload: { pr: "pr-7" },
  responseSchema: { type: "object", required: ["label"] },
  assignees: ["alice"],
  escalateTo: ["lead"],
  escalated: false,
  status: "pending",
  createdAtMs: NOW - 60_000,
  deadlineAtMs: NOW + 2 * 86_400_000,
  reminderCount: 1,
};

let root: Root;
let container: HTMLDivElement;
let client: QueryClient;
let posted: unknown[];
beforeEach(() => {
  vi.useFakeTimers({ now: NOW });
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  mocks.role = "contributor";
  posted = [];
  mocks.api.mockReset().mockImplementation(async (method: string, path: string, body?: unknown) => {
    if (method === "GET" && path === "/api/v1/universes/universe/inbox/requests?status=pending") return { requests: [pending] };
    if (method === "GET" && path === "/api/v1/universes/universe/inbox/requests/inp_1") return { request: pending };
    if (method === "POST" && path === "/api/v1/universes/universe/inbox/requests/inp_1/answer") {
      posted.push(body);
      return { request: { ...pending, status: "answered" } };
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
  vi.useRealTimers();
  vi.unstubAllGlobals();
});
async function settle() {
  for (let step = 0; step < 5; step++) await act(async () => { await vi.advanceTimersByTimeAsync(5); });
}
async function show(path: string) {
  await act(async () => root.render(
    <QueryClientProvider client={client}>
      <PermissionIdentityProvider userId="user" platformAdmin={false}>
        <MemoryRouter initialEntries={[path]}>
          <Routes>
            <Route path="u/:slug/inbox" element={<InboxPage admin={false} />} />
            <Route path="u/:slug/inbox/:requestId" element={<InboxPage admin={false} />} />
          </Routes>
        </MemoryRouter>
      </PermissionIdentityProvider>
    </QueryClientProvider>,
  ));
  await settle();
}
function button(label: string) {
  return [...document.body.querySelectorAll<HTMLButtonElement>("button")].find((node) => node.textContent?.trim() === label);
}
async function type(id: string, text: string) {
  const area = document.getElementById(id) as HTMLTextAreaElement;
  const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!;
  await act(async () => {
    setter.call(area, text);
    area.dispatchEvent(new Event("input", { bubbles: true }));
  });
}
async function click(label: string) {
  await act(async () => button(label)!.click());
  await settle();
}

it("lists pending requests and shows one with its context", async () => {
  await show("/u/test/inbox/inp_1");
  const text = document.body.textContent ?? "";
  expect(text).toContain("Merge pr-7?");
  expect(text).toContain("2d left · alice");
  expect(text).toContain('"pr": "pr-7"');
  expect(text).toContain("Escalates to");
  expect(text).toContain("review-session");
});

it("answers with a value that the request's schema expects", async () => {
  await show("/u/test/inbox/inp_1");
  await click("Approve");
  expect(document.body.textContent).toContain("needs an answer");
  expect(posted).toEqual([]);

  await type("inbox-answer-value", "{not json");
  await click("Approve");
  expect(document.body.textContent).toContain("not valid JSON");
  expect(posted).toEqual([]);

  await type("inbox-answer-value", '{"label": "ship"}');
  await type("inbox-answer-comment", "lgtm");
  await click("Approve");
  expect(posted).toEqual([{ approved: true, value: { label: "ship" }, comment: "lgtm" }]);
});

it("rejects without a value", async () => {
  await show("/u/test/inbox/inp_1");
  await click("Reject");
  expect(posted).toEqual([{ approved: false }]);
});

it("lets a viewer read but not answer", async () => {
  mocks.role = "viewer";
  await show("/u/test/inbox/inp_1");
  expect(document.body.textContent).toContain("You can read this request but not answer it.");
  expect(button("Approve")).toBeUndefined();
});

describe("labels", () => {
  it("counts down to the deadline", () => {
    expect(deadlineLabel(NOW - 1, NOW)).toBe("due now");
    expect(deadlineLabel(NOW + 30_000, NOW)).toBe("1m left");
    expect(deadlineLabel(NOW + 5 * 3_600_000, NOW)).toBe("5h left");
    expect(deadlineLabel(NOW + 8 * 86_400_000, NOW)).toBe("8d left");
  });

  it("parses an answer value", () => {
    expect(parseAnswerValue(' {"a": 1} ')).toEqual({ value: { a: 1 } });
    expect(typeof parseAnswerValue("")).toBe("string");
    expect(typeof parseAnswerValue("{")).toBe("string");
  });
});
