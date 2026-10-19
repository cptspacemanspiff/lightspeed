import type { ProgramCallView, ProgramNoteView } from "@lightspeed-ai/sdk";

/// One line of a program run's timeline: a call, narration, or a step with
/// what happened in it.
export type TimelineItem =
  | { kind: "call"; key: string; call: ProgramCallView }
  | { kind: "said"; key: string; note: ProgramNoteView }
  | {
      kind: "step";
      key: string;
      started: ProgramNoteView;
      ended: ProgramNoteView | null;
      items: TimelineItem[];
    };

/// Where an item falls in its execution: a note made after `k` calls comes
/// after call `k` and before call `k + 1`.
type Order = [execution: number, position: number, seq: number];

function callNumber(call: ProgramCallView): number {
  const number = Number(call.requestId.replace(/^call-/, ""));
  return Number.isFinite(number) ? number : 0;
}

function compare(a: Order, b: Order): number {
  return a[0] - b[0] || a[1] - b[1] || a[2] - b[2];
}

/// Arrange a run's notes and calls as the program made them: each step holds
/// its narration, calls and nested steps, in program order; what was made
/// outside every step, or in a step whose start is not loaded, is at the
/// top. Steps and calls are told apart per execution.
export function buildTimeline(notes: ProgramNoteView[], calls: ProgramCallView[]): TimelineItem[] {
  const stepKey = (execution: number, step: number) => `${execution}:${step}`;
  const steps = new Map<string, Extract<TimelineItem, { kind: "step" }>>();
  for (const note of notes) {
    if (note.kind === "stepStarted") {
      steps.set(stepKey(note.execution, note.seq), {
        kind: "step",
        key: `step-${note.execution}-${note.seq}`,
        started: note,
        ended: null,
        items: [],
      });
    }
  }
  const placed: { order: Order; item: TimelineItem; parent: string | null }[] = [];
  const parentOf = (execution: number, step: number | null | undefined) =>
    step !== undefined && step !== null && steps.has(stepKey(execution, step)) ? stepKey(execution, step) : null;
  for (const note of notes) {
    const order: Order = [note.execution, 2 * note.afterCalls, note.seq];
    if (note.kind === "stepStarted") {
      placed.push({
        order,
        item: steps.get(stepKey(note.execution, note.seq))!,
        parent: parentOf(note.execution, note.parent),
      });
    } else if (note.kind === "stepEnded") {
      const step = note.step !== undefined && note.step !== null ? steps.get(stepKey(note.execution, note.step)) : undefined;
      if (step) step.ended = note;
    } else {
      placed.push({
        order,
        item: { kind: "said", key: `said-${note.execution}-${note.seq}`, note },
        parent: parentOf(note.execution, note.step),
      });
    }
  }
  for (const call of calls) {
    placed.push({
      order: [call.execution, 2 * callNumber(call) - 1, 0],
      item: { kind: "call", key: call.id, call },
      parent: parentOf(call.execution, call.step),
    });
  }
  placed.sort((a, b) => compare(a.order, b.order));
  const top: TimelineItem[] = [];
  for (const { item, parent } of placed) {
    (parent ? steps.get(parent)!.items : top).push(item);
  }
  return top;
}
