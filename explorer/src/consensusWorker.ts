/// <reference lib="webworker" />

import init, {
  parse_finalized,
  parse_notarized,
  parse_seed,
} from "./alto_types/alto_types.js";

let publicKey: Uint8Array | undefined;
const initialized = init();
const worker = globalThis as unknown as DedicatedWorkerGlobalScope;

worker.onmessage = async (event: MessageEvent) => {
  if (event.data.type === "initialize") {
    publicKey = event.data.publicKey;
    return;
  }
  if (event.data.type !== "verify") {
    return;
  }

  await initialized;
  if (!publicKey) {
    throw new Error("consensus verifier was not initialized");
  }

  const { sequence, kind, payload, receivedAt } = event.data;
  let artifact = null;
  switch (kind) {
    case 0:
      artifact = parse_seed(publicKey, payload);
      break;
    case 1:
      artifact = parse_notarized(publicKey, payload);
      break;
    case 2:
      artifact = parse_finalized(publicKey, payload);
      break;
  }

  worker.postMessage({ sequence, kind, artifact, receivedAt });
};

export {};
