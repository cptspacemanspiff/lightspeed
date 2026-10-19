import { describe, expect, it } from "vitest";
import type { ProfileDocument, ProfileSummary } from "@/api";
import { copiedProfileDocument, profileGroups } from "./ProfilesPage";

describe("copiedProfileDocument", () => {
  it("copies setup fields without carrying registry identity", () => {
    const source: ProfileDocument = {
      profileId: "base-agent",
      displayName: "Base agent",
      description: "Shared agent setup",
      revision: 7,
      createdAtMs: 100,
      updatedAtMs: 200,
      metadata: { team: "platform" },
      config: {
        generation: { reasoningEffort: "high" },
        features: { timers: {} },
      },
      instructions: { type: "text", text: "Be useful." },
    };

    expect(copiedProfileDocument(source, "researcher", "Researcher")).toEqual({
      profileId: "researcher",
      displayName: "Researcher",
      description: "Shared agent setup",
      revision: 0,
      metadata: { team: "platform" },
      config: {
        generation: { reasoningEffort: "high" },
        features: { timers: {} },
      },
      instructions: { type: "text", text: "Be useful." },
    });
    expect(source.profileId).toBe("base-agent");
    expect(source.revision).toBe(7);
  });

  it("does not reuse the source display name when the new profile has none", () => {
    const source: ProfileDocument = {
      profileId: "base-agent",
      displayName: "Base agent",
      config: { features: { web: { fetch: {} } } },
    };

    expect(copiedProfileDocument(source, "copy", "")).toEqual({
      profileId: "copy",
      revision: 0,
      config: { features: { web: { fetch: {} } } },
    });
  });
});

describe("profileGroups", () => {
  const summary = (profileId: string, program?: boolean): ProfileSummary => ({
    profileId,
    revision: 1,
    updatedAtMs: 0,
    program,
  });

  it("lists agents and programs separately, each in API order", () => {
    const groups = profileGroups([
      summary("grader", true),
      summary("helper"),
      summary("checker", true),
      summary("writer", false),
    ]);
    expect(groups.map((group) => [group.label, group.profiles.map((p) => p.profileId)])).toEqual([
      ["Agents", ["helper", "writer"]],
      ["Programs", ["grader", "checker"]],
    ]);
  });

  it("leaves out an empty group", () => {
    expect(profileGroups([summary("helper")]).map((group) => group.label)).toEqual(["Agents"]);
    expect(profileGroups([])).toEqual([]);
  });
});
