/// The human-input inbox: questions program runs wait on. Answers are
/// checked loosely (the engine validates values against the schema); an
/// answer settles the request like the engine's `inbox/requests/answer`.
import { Hono } from "hono";
import type { InputRequestView } from "@/api";
import type { DemoStore } from "../store";
import { badRequest, conflict, notFound, readBody, universeFor } from "./common";

export function inboxRoutes(store: DemoStore): Hono {
  const app = new Hono();

  app.get("/:id/inbox/requests", (c) => {
    const universe = universeFor(store, c);
    if (!universe) return notFound(c);
    const status = c.req.query("status");
    const session = c.req.query("session");
    const requests = [...universe.inputRequests.values()]
      .filter((request) => (!status || request.status === status) && (!session || request.sessionId === session))
      .sort((a, b) => b.createdAtMs - a.createdAtMs);
    return c.json({ requests });
  });

  app.get("/:id/inbox/requests/:requestId", (c) => {
    const universe = universeFor(store, c);
    if (!universe) return notFound(c);
    const request = universe.inputRequests.get(c.req.param("requestId"));
    return request ? c.json({ request }) : notFound(c, "input request not found");
  });

  app.post("/:id/inbox/requests/:requestId/answer", async (c) => {
    const universe = universeFor(store, c);
    if (!universe) return notFound(c);
    const request = universe.inputRequests.get(c.req.param("requestId"));
    if (!request) return notFound(c, "input request not found");
    if (request.status !== "pending") return conflict(c, `input request ${request.requestId} is already ${request.status}`);
    const body = await readBody<{ approved?: unknown; value?: unknown; comment?: unknown }>(c);
    if (typeof body.approved !== "boolean") return badRequest(c, "approved is required");
    const hasValue = body.value !== undefined && body.value !== null;
    if (body.approved && Boolean(request.responseSchema) !== hasValue) {
      return badRequest(c, request.responseSchema
        ? "an approval of this request must carry a value matching its response schema"
        : "this request has no response schema; approve it without a value");
    }
    if (!body.approved && hasValue) return badRequest(c, "a rejection carries no value");
    const now = Date.now();
    const answered: InputRequestView = {
      ...request,
      status: "answered",
      closedAtMs: now,
      answer: {
        approved: body.approved,
        ...(hasValue ? { value: body.value } : {}),
        ...(typeof body.comment === "string" && body.comment ? { comment: body.comment } : {}),
        answeredBy: { kind: "actor", id: store.currentUser.id },
        answeredAtMs: now,
      },
    };
    universe.inputRequests.set(request.requestId, answered);
    return c.json({ request: answered });
  });

  return app;
}
