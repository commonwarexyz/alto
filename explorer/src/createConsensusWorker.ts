export const createConsensusWorker = (): Worker =>
  new Worker(new URL('./consensusWorker.ts', import.meta.url));
