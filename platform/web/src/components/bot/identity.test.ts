import { describe, expect, it } from "vitest";
import type { BotView } from "@/api";
import { botInputOf } from "./identity";

describe("botInputOf", () => {
  it("keeps the fields the bot pages don't edit, since a put replaces the whole document", () => {
    const bot = {
      botId: "triage",
      revision: 3,
      profileId: "triage",
      eventSeq: 0,
      createdAtMs: 1,
      updatedAtMs: 1,
      mcpCredentials: { "github-issues": "sublease.issues.0123" },
    } as BotView;

    expect(botInputOf(bot).mcpCredentials).toEqual({ "github-issues": "sublease.issues.0123" });
    expect(botInputOf({ ...bot, mcpCredentials: undefined }).mcpCredentials).toEqual({});
  });
});
