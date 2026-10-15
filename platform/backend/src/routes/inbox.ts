import type { InboxRequestAnswerParams, InboxRequestListParams, InputRequestStatus } from "@lightspeed-ai/sdk";
import { Hono } from "hono";
import type { AppContext, ApiVariables } from "../context.js";
import { engineClientFor, withGateway } from "./gateway.js";
import { universeForSession } from "./universes.js";

// The universe's human-input inbox: questions program runs are waiting on.
// The runtime authorizes each request through its session.

const STATUSES: readonly string[] = ["pending", "answered", "expired", "cancelled"];

/** The list filters from a query string, or why they are invalid. */
export function inboxListParams(query: {
  status?: string;
  session?: string;
  limit?: string;
}): InboxRequestListParams | string {
  const { status, session, limit } = query;
  if (status !== undefined && !STATUSES.includes(status)) return "invalid status";
  if (limit !== undefined && !/^[1-9]\d{0,3}$/.test(limit)) return "invalid limit";
  return {
    ...(status ? { status: status as InputRequestStatus } : {}),
    ...(session ? { session } : {}),
    ...(limit ? { limit: Number(limit) } : {}),
  };
}

/** An answer from a request body, or null when the body is not one. */
export function inboxAnswerParams(requestId: string, body: unknown): InboxRequestAnswerParams | null {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return null;
  const { approved, value, comment } = body as Record<string, unknown>;
  if (typeof approved !== "boolean") return null;
  if (comment !== undefined && comment !== null && typeof comment !== "string") return null;
  return {
    requestId,
    approved,
    ...(value !== undefined && value !== null ? { value: value as InboxRequestAnswerParams["value"] } : {}),
    ...(typeof comment === "string" && comment.trim() ? { comment } : {}),
  };
}

export function inboxRoutes(ctx: AppContext) {
  const app = new Hono<{ Variables: ApiVariables }>();

  app.get("/:id/inbox/requests", async (c) => {
    const access = await universeForSession(ctx, c, c.req.param("id"));
    if (!access) {
      return c.json({ error: "not found" }, 404);
    }
    const params = inboxListParams({
      status: c.req.query("status"),
      session: c.req.query("session"),
      limit: c.req.query("limit"),
    });
    if (typeof params === "string") {
      return c.json({ error: params }, 400);
    }
    return withGateway(c, async () => {
      const client = engineClientFor(ctx, access);
      const response = await client.call("inbox/requests/list", params);
      return c.json(response.result);
    });
  });

  app.get("/:id/inbox/requests/:requestId", async (c) => {
    const access = await universeForSession(ctx, c, c.req.param("id"));
    if (!access) {
      return c.json({ error: "not found" }, 404);
    }
    return withGateway(c, async () => {
      const client = engineClientFor(ctx, access);
      const response = await client.call("inbox/requests/read", {
        requestId: c.req.param("requestId"),
      });
      return c.json(response.result);
    });
  });

  app.post("/:id/inbox/requests/:requestId/answer", async (c) => {
    const access = await universeForSession(ctx, c, c.req.param("id"));
    if (!access) {
      return c.json({ error: "not found" }, 404);
    }
    let body: unknown = null;
    try {
      body = await c.req.json();
    } catch {
      // An unreadable body is an invalid one.
    }
    const params = inboxAnswerParams(c.req.param("requestId"), body);
    if (!params) {
      return c.json({ error: "invalid body" }, 400);
    }
    return withGateway(c, async () => {
      const client = engineClientFor(ctx, access);
      const response = await client.call("inbox/requests/answer", params);
      return c.json(response.result);
    });
  });

  return app;
}
