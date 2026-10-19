// @vitest-environment jsdom
import { act, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { ProgramSpec } from "@lightspeed-ai/sdk";
import { ProgramSection } from "./program-section";

let root: Root;
let container: HTMLDivElement;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

async function render(initial: ProgramSpec) {
  const state: { program: ProgramSpec; error: string | null } = { program: initial, error: null };
  function Harness() {
    const [program, setProgram] = useState(initial);
    state.program = program;
    return (
      <ProgramSection
        program={program}
        onChange={setProgram}
        readOnly={false}
        onValidityChange={(error) => { state.error = error; }}
      />
    );
  }
  await act(async () => root.render(<Harness />));
  return state;
}

async function edit(selector: string, value: string) {
  const element = container.querySelector<HTMLInputElement | HTMLTextAreaElement>(selector)!;
  expect(element).not.toBeNull();
  const prototype = element instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
  await act(async () => {
    Object.getOwnPropertyDescriptor(prototype, "value")!.set!.call(element, value);
    element.dispatchEvent(new Event("input", { bubbles: true }));
  });
}

it("edits the source, required contracts, schemas and limits", async () => {
  const state = await render({ source: { type: "text", text: "export default {}" }, semanticsVersion: 1 });
  await edit("#program-source", "export default { async run() { return 1; } }");
  await edit("#program-requires", "time.sleep\n\nweb.fetch sha256:abc\n");
  await edit("#program-inputSchema", '{"type":"object","required":["claim"]}');
  await edit("#program-limit-maxWorkflowCalls", "2000");
  expect(state.program).toEqual({
    source: { type: "text", text: "export default { async run() { return 1; } }" },
    semanticsVersion: 1,
    requires: [{ contract: "time.sleep" }, { contract: "web.fetch", digest: "sha256:abc" }],
    inputSchema: { type: "object", required: ["claim"] },
    limits: { maxWorkflowCalls: 2000 },
  });

  // Clearing a limit or a schema removes it.
  await edit("#program-limit-maxWorkflowCalls", "");
  await edit("#program-inputSchema", "");
  expect(state.program.limits).toBeUndefined();
  expect(state.program.inputSchema).toBeUndefined();
});

it("reports a schema that is not a JSON object and keeps the last good one", async () => {
  const state = await render({
    source: { type: "text", text: "" },
    outputSchema: { type: "object" },
  });
  await edit("#program-outputSchema", "{ not json");
  expect(state.error).toMatch(/^output schema: /);
  expect(state.program.outputSchema).toEqual({ type: "object" });
  await edit("#program-outputSchema", "[1]");
  expect(state.error).toBe("output schema: a schema is a JSON object");
  await edit("#program-outputSchema", '{"type":"string"}');
  expect(state.error).toBeNull();
  expect(state.program.outputSchema).toEqual({ type: "string" });
});

it("leaves a stored source to the JSON tab", async () => {
  await render({ source: { type: "textRef", blobRef: "sha256:source" } });
  expect(container.querySelector("#program-source")).toBeNull();
  expect(container.textContent).toContain("sha256:source");
});
