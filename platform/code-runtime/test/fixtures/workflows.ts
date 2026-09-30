// Workflows standing in for the systems a manifest can point at.

import {
  condition,
  defineSignal,
  getExternalWorkflowHandle,
  setHandler,
  sleep,
} from "@temporalio/workflow";
import {
  CALL_COMPLETED_SIGNAL,
  CALL_REQUESTED_SIGNAL,
  type HostCallCompleted,
  type Json,
  type ToolCallRequest,
} from "../../src/contract.js";

/** A `workflow` target slow enough to outlive a short hold. */
export async function slowDouble(request: ToolCallRequest): Promise<Json> {
  const { n, delay_ms } = request.arguments as { n: number; delay_ms?: number };
  await sleep(delay_ms ?? 1_500);
  return { doubled: n * 2 };
}

/** A `host` target: answers each requested call by signal. */
export async function toolHost(): Promise<void> {
  const requests: ToolCallRequest[] = [];
  let stopped = false;
  setHandler(defineSignal<[ToolCallRequest]>(CALL_REQUESTED_SIGNAL), (request) => {
    requests.push(request);
  });
  setHandler(defineSignal("stop"), () => {
    stopped = true;
  });
  while (!stopped) {
    await condition(() => stopped || requests.length > 0);
    for (const request of requests.splice(0)) {
      const completed: HostCallCompleted = {
        call_id: request.call_id,
        outcome: { ok: true, value: { handled: request.tool, by: request.context } },
      };
      await getExternalWorkflowHandle(request.execution_id).signal(
        CALL_COMPLETED_SIGNAL,
        completed,
      );
    }
  }
}
