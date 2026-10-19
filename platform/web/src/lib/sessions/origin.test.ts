import { expect, it } from "vitest";
import { childNoun, childrenNoun } from "./origin";

it("calls a program's child a sub-program and every other child a sub-agent", () => {
  expect(childNoun({ kind: "subprogram" })).toBe("sub-program");
  expect(childNoun({ kind: "subagent" })).toBe("sub-agent");
  expect(childNoun(null)).toBe("sub-agent");
});

it("names a list of children by the kinds it holds", () => {
  const agent = { origin: { kind: "subagent" as const } };
  const program = { origin: { kind: "subprogram" as const } };
  expect(childrenNoun([agent, agent])).toBe("sub-agents");
  expect(childrenNoun([program])).toBe("sub-programs");
  expect(childrenNoun([agent, program])).toBe("sub-agents and sub-programs");
  expect(childrenNoun([])).toBe("sub-agents");
});
