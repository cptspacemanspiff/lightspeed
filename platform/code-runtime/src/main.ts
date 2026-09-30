import { Client, Connection } from "@temporalio/client";
import { NativeConnection } from "@temporalio/worker";
import { createCodeRuntimeWorker } from "./worker.js";

const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233";
const namespace = process.env.TEMPORAL_NAMESPACE ?? "default";

const connection = await NativeConnection.connect({ address });
const client = new Client({ connection: await Connection.connect({ address }), namespace });
const worker = await createCodeRuntimeWorker({ connection, client, namespace });
await worker.run();
