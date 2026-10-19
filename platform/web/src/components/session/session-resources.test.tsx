import { createElement } from "react";
import { renderToString } from "react-dom/server";
import { expect, it } from "vitest";
import { sessionResources } from "@/lib/sessions/session-resources";
import { SessionResourceChips } from "./session-resources";

it("shows a Code mode resource chip only when code mode is enabled", () => {
  const render = (features: unknown) => renderToString(createElement(SessionResourceChips, {
    resources: sessionResources({ features }, null),
    slug: "demo",
  }));

  const enabled = render({ codeMode: {} });
  expect(enabled).toContain('aria-label="Code mode: Code mode"');

  const disabled = render({});
  expect(disabled).not.toContain('aria-label="Code mode: Code mode"');
});

it("counts the programs a sub-agents grant lists beside its agents", () => {
  const render = (subagents: unknown) => renderToString(createElement(SessionResourceChips, {
    resources: sessionResources({ features: { subagents } }, null),
    slug: "demo",
  }));
  expect(render({ agents: [], programs: [{ profileId: "claim-check" }] })).toContain("1 program");
  expect(render({ agents: [{ profileId: "reviewer" }], programs: [{ profileId: "claim-check" }] })).toContain("1 agent, 1 program");
  expect(render({ agents: [] })).toContain("0 agents");
});
