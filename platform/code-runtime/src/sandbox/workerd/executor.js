// Runs inside workerd. One WebSocket connection is one sandbox session:
// the host sends the script module, this worker loads it into a fresh
// isolate with no network access, and relays the isolate's steps.

import { RpcTarget } from "cloudflare:workers";

const COMPATIBILITY_DATE = "2026-09-01";

// Passed to the loaded isolate by reference. It lives in the connection's
// request context, so it can use the connection's socket.
class Host extends RpcTarget {
  #socket;
  #waiting;

  constructor(socket) {
    super();
    this.#socket = socket;
  }

  // The isolate reports what it did since its last step, then waits here
  // for the one result the host decides to hand it next.
  step(batch) {
    this.#socket.send(JSON.stringify({ type: "step", ...batch }));
    if (batch.done) return null;
    return new Promise((resolve) => {
      this.#waiting = resolve;
    });
  }

  deliver(reply) {
    const waiting = this.#waiting;
    this.#waiting = undefined;
    waiting?.(reply);
  }
}

export default {
  async fetch(request, env, ctx) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("expected a websocket upgrade", { status: 426 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    const host = new Host(server);
    let started = false;

    server.addEventListener("message", (event) => {
      const message = JSON.parse(event.data);
      if (message.type === "deliver") {
        host.deliver(message.reply);
      } else if (message.type === "start" && !started) {
        started = true;
        const worker = env.LOADER.get(crypto.randomUUID(), () => ({
          compatibilityDate: COMPATIBILITY_DATE,
          mainModule: "script.js",
          modules: { "script.js": message.module },
          env: {},
          globalOutbound: null,
        }));
        ctx.waitUntil(
          worker
            .getEntrypoint()
            .run(host, message.init)
            .catch((error) => {
              server.send(
                JSON.stringify({
                  type: "error",
                  name: String(error?.name ?? "Error"),
                  message: String(error?.message ?? error),
                  stack: String(error?.stack ?? ""),
                }),
              );
            }),
        );
      }
    });
    return new Response(null, { status: 101, webSocket: client });
  },
};
