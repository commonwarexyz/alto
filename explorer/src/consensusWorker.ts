/// <reference lib="webworker" />

import init, {
  parse_finalized,
  parse_notarized,
} from "./alto_types/alto_types.js";

const initialized = init();
const worker = globalThis as unknown as DedicatedWorkerGlobalScope;

worker.onmessage = async (event: MessageEvent) => {
  const { kind, payload, publicKey } = event.data;

  // Always reply, including after WASM initialization failure or panic, so the pool can advance
  // its ordered results and replace a failed worker.
  let artifact = null;
  let error: string | undefined;
  try {
    await initialized;
    switch (kind) {
      case 1:
        artifact = parse_notarized(publicKey, payload);
        break;
      case 2:
        artifact = parse_finalized(publicKey, payload);
        break;
    }
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
  }

  worker.postMessage({ artifact, error });
};

export {};
