import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const EXECUTOR_SOURCE = new URL("./executor.js", import.meta.url);
const MEBIBYTE = 1024 * 1024;
const MIN_HEAP_MB = 16;

const configs = new Map<number, string>();

/**
 * Path of a workerd configuration whose isolates are capped at
 * `memoryBytes` of heap. workerd reads embedded files relative to the
 * configuration, so each one is written beside a copy of the executor.
 */
export function configPath(memoryBytes: number): string {
  const heapMb = Math.max(MIN_HEAP_MB, Math.ceil(memoryBytes / MEBIBYTE));
  let path = configs.get(heapMb);
  if (!path) {
    const directory = mkdtempSync(join(tmpdir(), "code-runtime-workerd-"));
    writeFileSync(join(directory, "executor.js"), readFileSync(EXECUTOR_SOURCE));
    path = join(directory, "config.capnp");
    writeFileSync(path, configText(heapMb));
    configs.set(heapMb, path);
  }
  return path;
}

// The executor worker holds the Worker Loader binding and nothing else: no
// outbound network service, no storage, and no bindings a loaded script
// could inherit. The socket address is supplied on the command line so each
// process gets its own loopback port.
function configText(heapMb: number): string {
  return `using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [ (name = "executor", worker = .executor) ],
  sockets = [ (name = "http", http = (), service = "executor") ],
  # A script that outgrows the heap aborts its own process.
  v8Flags = ["--max-heap-size=${heapMb}"],
);

const executor :Workerd.Worker = (
  modules = [ (name = "executor.js", esModule = embed "executor.js") ],
  compatibilityDate = "2026-09-01",
  # Worker Loader is an experimental binding in self-hosted workerd.
  compatibilityFlags = ["experimental"],
  bindings = [ (name = "LOADER", workerLoader = ()) ],
);
`;
}
