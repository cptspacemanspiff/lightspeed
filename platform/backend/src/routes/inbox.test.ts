import { describe, expect, it } from "vitest";
import { inboxAnswerParams, inboxListParams } from "./inbox.js";

describe("inbox list filters", () => {
  it("passes known filters and drops empty ones", () => {
    expect(inboxListParams({})).toEqual({});
    expect(inboxListParams({ status: "pending", session: "s1", limit: "20" })).toEqual({
      status: "pending",
      session: "s1",
      limit: 20,
    });
    expect(inboxListParams({ session: "" })).toEqual({});
  });

  it("refuses unknown states and malformed limits", () => {
    expect(inboxListParams({ status: "open" })).toBe("invalid status");
    expect(inboxListParams({ limit: "0" })).toBe("invalid limit");
    expect(inboxListParams({ limit: "-5" })).toBe("invalid limit");
    expect(inboxListParams({ limit: "ten" })).toBe("invalid limit");
  });
});

describe("inbox answers", () => {
  it("carries a decision with its optional value and comment", () => {
    expect(inboxAnswerParams("inp_1", { approved: false })).toEqual({ requestId: "inp_1", approved: false });
    expect(inboxAnswerParams("inp_1", { approved: true, value: { label: "ship" }, comment: "lgtm" })).toEqual({
      requestId: "inp_1",
      approved: true,
      value: { label: "ship" },
      comment: "lgtm",
    });
    // A blank comment is no comment.
    expect(inboxAnswerParams("inp_1", { approved: true, comment: "  " })).toEqual({
      requestId: "inp_1",
      approved: true,
    });
  });

  it("refuses bodies that are not a decision", () => {
    for (const body of [null, [], "yes", {}, { approved: "true" }, { approved: true, comment: 7 }]) {
      expect(inboxAnswerParams("inp_1", body)).toBeNull();
    }
  });
});
