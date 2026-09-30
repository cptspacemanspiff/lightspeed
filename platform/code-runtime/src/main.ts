import { Client, Connection } from "@temporalio/client";
import { NativeConnection } from "@temporalio/worker";
import { createCodeRuntimeWorker } from "./worker.js";

const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233";
const namespace = process.env.TEMPORAL_NAMESPACE ?? "default";
// The same variable tells Lightspeed which queue to start scripts on.
const taskQueue = process.env.LIGHTSPEED_CODE_RUNTIME_TASK_QUEUE || undefined;

const connection = await NativeConnection.connect({ address });
const client = new Client({ connection: await Connection.connect({ address }), namespace });
const worker = await createCodeRuntimeWorker({
  connection,
  client,
  namespace,
  ...(taskQueue ? { taskQueue } : {}),
});
await worker.run();
