/// <reference lib="webworker" />

import init, {
  parse_finalized,
  parse_notarized,
  parse_seed,
} from "./alto_types/alto_types.js";

let publicKey: Uint8Array | undefined;
let standard = false;
const initialized = init();
const worker = globalThis as unknown as DedicatedWorkerGlobalScope;

worker.onmessage = async (event: MessageEvent) => {
  if (event.data.type === "initialize") {
    publicKey = event.data.publicKey;
    standard = event.data.standard;
    return;
  }
  if (event.data.type !== "verify") {
    return;
  }

  const { sequence, kind, payload, receivedAt } = event.data;

  // Always answer: the pool releases results strictly in sequence order, so a job that never
  // replies (a failed wasm load, a wasm panic, a missing key) would block every later artifact.
  let artifact = null;
  let error: string | undefined;
  try {
    await initialized;
    if (!publicKey) {
      throw new Error("consensus verifier was not initialized");
    }
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

  worker.postMessage({ sequence, kind, artifact, receivedAt, error });
};

export {};
