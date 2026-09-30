import { fileURLToPath } from "node:url";
import type { Client } from "@temporalio/client";
import { Worker, type NativeConnection } from "@temporalio/worker";
import { createActivities } from "./activities.js";
import { CODE_RUNTIME_TASK_QUEUE } from "./contract.js";
import type { Sandbox } from "./sandbox/types.js";

export interface CodeRuntimeWorkerOptions {
  connection: NativeConnection;
  /** Client on the same namespace; activities use it to reach their workflow. */
  client: Client;
  namespace?: string;
  taskQueue?: string;
  sandbox?: Sandbox;
}

export function createCodeRuntimeWorker(options: CodeRuntimeWorkerOptions): Promise<Worker> {
  return Worker.create({
    connection: options.connection,
    namespace: options.namespace ?? "default",
    taskQueue: options.taskQueue ?? CODE_RUNTIME_TASK_QUEUE,
    workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)),
    activities: createActivities({
      client: options.client,
      ...(options.sandbox ? { sandbox: options.sandbox } : {}),
    }),
  });
}
