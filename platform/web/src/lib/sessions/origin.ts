import type { SessionOrigin } from "@/api";

/// What a child session is called. A program's child says so in its origin;
/// every other child, including program children created before origins
/// said which they were, is a sub-agent.
export function childNoun(origin: Pick<SessionOrigin, "kind"> | null | undefined): "sub-agent" | "sub-program" {
  return origin?.kind === "subprogram" ? "sub-program" : "sub-agent";
}

/// The plural for a list of children: "sub-agents", "sub-programs", or
/// "sub-agents and sub-programs" when the list holds both.
export function childrenNoun(children: readonly { origin?: Pick<SessionOrigin, "kind"> | null }[]): string {
  const nouns = new Set(children.map((child) => childNoun(child.origin)));
  if (nouns.size > 1) return "sub-agents and sub-programs";
  return nouns.has("sub-program") ? "sub-programs" : "sub-agents";
}

export function capitalize(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}
