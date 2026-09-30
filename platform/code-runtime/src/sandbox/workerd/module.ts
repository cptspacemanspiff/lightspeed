import { PRELUDE } from "../prelude.js";

/**
 * The ES module loaded into the sandbox isolate. Nothing runs at module
 * scope: `run` installs the host functions the shared prelude expects,
 * starts the script, and then reports each time the script comes to rest.
 */
export function scriptModule(code: string): string {
  return `import { WorkerEntrypoint } from "cloudflare:workers";

${code}

function __prelude() {
${PRELUDE}
}

export default class extends WorkerEntrypoint {
  async run(host, init) {
    // Captured before the prelude removes timers from the script's reach.
    const defer = globalThis.setTimeout.bind(globalThis);
    const resolvers = new Map();
    let issued = [];
    let logs = [];
    let nextId = 0;
    let clock = init.startClock;
    let done;

    globalThis.__hostCall = (tool, argumentsJson) =>
      new Promise((resolve) => {
        const id = nextId++;
        resolvers.set(id, resolve);
        issued.push({ id, tool, argumentsJson });
      });
    globalThis.__hostLog = (level, text) => { logs.push({ level, text }); };
    globalThis.__hostClock = () => clock;
    globalThis.__hostDone = (ok, json) => { done ??= { ok, json }; };
    globalThis.__toolNames = JSON.stringify(init.toolNames);
    globalThis.__inputJson = init.inputJson;
    globalThis.__seed0 = init.seed[0];
    globalThis.__seed1 = init.seed[1];
    __prelude();
    globalThis.__start(__main);

    for (;;) {
      // A timer task runs only after every pending microtask, so by then
      // the script can make no progress without a result.
      await new Promise((resolve) => defer(resolve, 0));
      const batch = { issued, logs, done };
      issued = [];
      logs = [];
      const reply = await host.step(batch);
      if (!reply) return;
      clock = reply.clock;
      const resolve = resolvers.get(reply.id);
      resolvers.delete(reply.id);
      resolve(reply.outcomeJson);
    }
  }
}
`;
}
