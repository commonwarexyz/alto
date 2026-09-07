/// <reference lib="webworker" />

import init, {
  parse_finalized,
  parse_notarized,
  parse_seed,
} from "./alto_types/alto_types.js";

const initialized = init();
const worker = globalThis as unknown as DedicatedWorkerGlobalScope;

worker.onmessage = async (event: MessageEvent) => {
  const { kind, payload, publicKey, standard } = event.data;

  // Always answer: the pool releases results strictly in sequence order, so a job that never
  // replies (a failed wasm load or a wasm panic) would block every later artifact.
  let artifact = null;
  let error: string | undefined;
  try {
    await initialized;
    switch (kind) {
      case 0:
        artifact = parse_seed(publicKey, payload);
        break;
      case 1:
        artifact = parse_notarized(publicKey, payload, standard);
        break;
      case 2:
        artifact = parse_finalized(publicKey, payload, standard);
        break;
    }
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
  }

  worker.postMessage({ artifact, error });
};

export {};
