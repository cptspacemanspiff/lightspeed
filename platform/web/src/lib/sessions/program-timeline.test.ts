import { expect, it } from "vitest";
import type { ProgramCallView, ProgramNoteView } from "@lightspeed-ai/sdk";
import { buildTimeline, type TimelineItem } from "./program-timeline";

function note(fields: Partial<ProgramNoteView> & Pick<ProgramNoteView, "seq" | "kind" | "afterCalls">): ProgramNoteView {
  return { execution: 0, recordedAtMs: 0, ...fields };
}

function call(requestId: string, step?: number, execution = 0): ProgramCallView {
  return {
    id: `code-tool:${execution}:${requestId}`,
    execution,
    requestId,
    tool: "web_fetch",
    status: "succeeded",
    arguments: { contentRef: "sha256:a", preview: "{}" },
    admittedAtMs: 0,
    ...(step ? { step } : {}),
  };
}

/// The tree as labels: a call by request id, narration by its text, a step
/// by its name with its contents.
function shape(items: TimelineItem[]): unknown[] {
  return items.map((item) =>
    item.kind === "call"
      ? item.call.requestId
      : item.kind === "said"
        ? `"${item.note.text}"`
        : { [item.started.text!]: shape(item.items), ended: item.ended !== null });
}

it("nests each step's narration, calls and steps in program order", () => {
  const notes = [
    note({ seq: 1, kind: "said", text: "start", afterCalls: 0 }),
    note({ seq: 2, kind: "stepStarted", text: "check a", afterCalls: 0 }),
    note({ seq: 3, kind: "stepStarted", text: "check b", afterCalls: 0 }),
    note({ seq: 4, kind: "said", step: 2, text: "fetched a", afterCalls: 2 }),
    note({ seq: 5, kind: "stepStarted", parent: 2, text: "deeper", afterCalls: 2 }),
    note({ seq: 6, kind: "stepEnded", step: 5, afterCalls: 3 }),
    note({ seq: 7, kind: "stepEnded", step: 2, afterCalls: 3 }),
  ];
  const calls = [call("call-1", 2), call("call-2", 3), call("call-3", 5), call("call-4")];
  expect(shape(buildTimeline(notes, calls))).toEqual([
    '"start"',
    { "check a": ["call-1", '"fetched a"', { deeper: ["call-3"], ended: true }], ended: true },
    { "check b": ["call-2"], ended: false },
    "call-4",
  ]);
});

it("keeps each execution's steps apart and puts calls of unknown steps at the top", () => {
  const notes = [
    note({ seq: 1, kind: "stepStarted", text: "first", afterCalls: 0 }),
    note({ execution: 1, seq: 1, kind: "stepStarted", text: "second", afterCalls: 0 }),
  ];
  const calls = [call("call-1", 1), call("call-1", 1, 1), call("call-2", 9, 1)];
  expect(shape(buildTimeline(notes, calls))).toEqual([
    { first: ["call-1"], ended: false },
    { second: ["call-1"], ended: false },
    "call-2",
  ]);
});
