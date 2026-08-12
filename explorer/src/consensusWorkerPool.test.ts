import { expect, test } from "@jest/globals";
import { ConsensusWorkerPool, consensusWorkerCount } from "./consensusWorkerPool";

class FakeWorker {
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: ErrorEvent) => void) | null = null;
  messages: unknown[] = [];
  completed = 0;
  terminated = false;

  postMessage(message: unknown) {
    this.messages.push(message);
  }

  terminate() {
    this.terminated = true;
  }

  fail() {
    this.onerror?.({ preventDefault: () => undefined } as unknown as ErrorEvent);
  }

  completeNext() {
    const message = this.messages.slice(1)[this.completed] as any;
    this.completed += 1;
    this.onmessage?.({
      data: {
        sequence: message.sequence,
        kind: message.kind,
        artifact: null,
        receivedAt: message.receivedAt,
      },
    } as MessageEvent);
  }
}

test("dispatches every artifact across the verifier pool without sampling", () => {
  const workers = [new FakeWorker(), new FakeWorker(), new FakeWorker()];
  const verifiedReceivedAt: number[] = [];
  const pool = new ConsensusWorkerPool(
    workers as unknown as Worker[],
    new Uint8Array([1, 2, 3]),
    (result) => verifiedReceivedAt.push(result.receivedAt),
  );

  for (let id = 0; id < 8; id++) {
    pool.verify(id % 3, new Uint8Array([id]), id);
  }

  expect(workers.map((worker) => worker.messages.slice(1).length)).toEqual([1, 1, 1]);

  // A worker that finishes early immediately takes the next queued artifact.
  for (let id = 0; id < 6; id++) {
    workers[0].completeNext();
  }
  workers[1].completeNext();
  workers[2].completeNext();

  const verificationMessages = workers.flatMap((worker) => worker.messages.slice(1));
  expect(verificationMessages).toHaveLength(8);
  expect(verificationMessages.map((message: any) => message.payload[0]).sort()).toEqual([
    0, 1, 2, 3, 4, 5, 6, 7,
  ]);
  expect(verifiedReceivedAt).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);

  pool.terminate();
  expect(workers.every((worker) => worker.terminated)).toBe(true);
});

test("reserves a core while using enough parallel verifiers", () => {
  expect(consensusWorkerCount(2)).toBe(2);
  expect(consensusWorkerCount(8)).toBe(7);
  expect(consensusWorkerCount(18)).toBe(16);
  expect(consensusWorkerCount(32)).toBe(16);
});

test("retries an in-flight artifact on a replacement worker", () => {
  const worker = new FakeWorker();
  const replacements: FakeWorker[] = [];
  const verifiedReceivedAt: number[] = [];
  const errors: ErrorEvent[] = [];
  const pool = new ConsensusWorkerPool(
    [worker] as unknown as Worker[],
    new Uint8Array([1, 2, 3]),
    (result) => verifiedReceivedAt.push(result.receivedAt),
    (error) => errors.push(error),
    () => {
      const replacement = new FakeWorker();
      replacements.push(replacement);
      return replacement as unknown as Worker;
    },
  );

  pool.verify(1, new Uint8Array([9]), 42);
  worker.fail();

  expect(worker.terminated).toBe(true);
  expect(replacements).toHaveLength(1);
  expect((replacements[0].messages[1] as any).payload[0]).toBe(9);

  replacements[0].completeNext();
  expect(verifiedReceivedAt).toEqual([42]);
  expect(errors).toEqual([]);
});
