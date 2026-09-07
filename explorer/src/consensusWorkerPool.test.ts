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

  completeNext(error?: string) {
    const message = this.messages[this.completed] as any;
    this.completed += 1;
    this.onmessage?.({ data: { sequence: message.sequence, artifact: null, error } } as MessageEvent);
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

  expect(workers.map((worker) => worker.messages.length)).toEqual([1, 1, 1]);

  // A worker that finishes early immediately takes the next queued artifact.
  for (let id = 0; id < 6; id++) {
    workers[0].completeNext();
  }
  workers[1].completeNext();
  workers[2].completeNext();

  const verificationMessages = workers.flatMap((worker) => worker.messages);
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

test("keeps delivering recent results when an earlier worker stays pending", () => {
  const stalled = new FakeWorker();
  const healthy = new FakeWorker();
  const delivered: number[] = [];
  const skipped: number[] = [];
  const pool = new ConsensusWorkerPool(
    [stalled, healthy] as unknown as Worker[],
    new Uint8Array([1]),
    result => {
      if (result.skipped) skipped.push(result.skipped);
      delivered.push(result.receivedAt);
    },
    undefined, undefined, false, 4,
  );
  pool.verify(1, new Uint8Array([0]), 0);
  for (let id = 1; id < 40; id++) {
    pool.verify(1, new Uint8Array([id]), id);
    healthy.completeNext();
  }
  expect(delivered).toEqual(Array.from({ length: 39 }, (_, i) => i + 1));
  expect(skipped).toEqual([1]);

  // A late response cannot resurrect work already reported as skipped
  stalled.completeNext();
  expect(delivered).toHaveLength(39);
  expect(pool.droppedCount()).toBe(1);
  pool.terminate();
});

test("retries an in-flight artifact on a replacement worker", () => {
  const worker = new FakeWorker();
  const replacements: FakeWorker[] = [];
  const verifiedReceivedAt: number[] = [];
  const errors: (ErrorEvent | Error)[] = [];
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
  expect((replacements[0].messages[0] as any).payload[0]).toBe(9);

  replacements[0].completeNext();
  expect(verifiedReceivedAt).toEqual([42]);
  expect(errors).toEqual([]);
});

test("a failed verification still releases later results in order and replaces the worker", () => {
  // The pool swaps replacements into the array it is given, so keep our own references.
  const first = new FakeWorker();
  const second = new FakeWorker();
  const workers = [first, second];
  const replacements: FakeWorker[] = [];
  const verifiedReceivedAt: number[] = [];
  const errors: (ErrorEvent | Error)[] = [];
  const pool = new ConsensusWorkerPool(
    workers as unknown as Worker[],
    new Uint8Array([1, 2, 3]),
    (result) => verifiedReceivedAt.push(result.receivedAt),
    (error) => errors.push(error),
    () => {
      const replacement = new FakeWorker();
      replacements.push(replacement);
      return replacement as unknown as Worker;
    },
  );
  pool.verify(1, new Uint8Array([0]), 0);
  pool.verify(1, new Uint8Array([1]), 1);

  // Hold the second artifact's result until the first reports
  second.completeNext();
  expect(verifiedReceivedAt).toEqual([]);
  first.completeNext("wasm panic");
  expect(verifiedReceivedAt).toEqual([0, 1]);

  // Retire the failed worker (its WASM may be poisoned) and refill its slot without retrying the
  // released result
  expect(first.terminated).toBe(true);
  expect(replacements).toHaveLength(1);
  expect(replacements[0].messages).toHaveLength(0);
  expect(errors).toEqual([]);

  // A successful verification resets the failure count. Repeated failures without one stop the pool
  pool.verify(1, new Uint8Array([2]), 2);
  second.completeNext();
  expect(verifiedReceivedAt).toEqual([0, 1, 2]);
  for (let id = 3; id < 6; id++) {
    pool.verify(1, new Uint8Array([id]), id);
    const active = [second, ...replacements].find(
      (worker) => !worker.terminated && worker.messages.length > worker.completed,
    )!;
    active.completeNext("wasm init failed");
  }
  expect(verifiedReceivedAt).toEqual([0, 1, 2, 3, 4, 5]);
  expect(errors).toHaveLength(1);
  expect([first, second, ...replacements].every((worker) => worker.terminated)).toBe(true);
});

test("an idle worker that fails is removed and replaced, and repeated failures stop the pool", () => {
  const worker = new FakeWorker();
  const replacements: FakeWorker[] = [];
  const errors: (ErrorEvent | Error)[] = [];
  const pool = new ConsensusWorkerPool(
    [worker] as unknown as Worker[],
    new Uint8Array([1, 2, 3]),
    () => undefined,
    (error) => errors.push(error),
    () => {
      const replacement = new FakeWorker();
      replacements.push(replacement);
      return replacement as unknown as Worker;
    },
  );

  // Fails before any artifact was dispatched (e.g. the worker script did not load).
  worker.fail();
  expect(worker.terminated).toBe(true);
  expect(replacements).toHaveLength(1);

  // Work goes to the replacement, never to the dead worker.
  pool.verify(1, new Uint8Array([7]), 7);
  expect(worker.messages).toHaveLength(0);
  expect(replacements[0].messages).toHaveLength(1);

  // Consecutive failures without progress eventually surface an error instead of respawning forever.
  replacements[0].fail();
  replacements[1].fail();
  expect(errors).toHaveLength(1);
  expect(replacements.every((replacement) => replacement.terminated)).toBe(true);
});

test("bounds pending work when every worker stalls", () => {
  const worker = new FakeWorker();
  const verifiedReceivedAt: number[] = [];
  const skipped: number[] = [];
  const pool = new ConsensusWorkerPool(
    [worker] as unknown as Worker[],
    new Uint8Array([1, 2, 3]),
    (result) => {
      if (result.skipped) skipped.push(result.skipped);
      verifiedReceivedAt.push(result.receivedAt);
    },
    undefined,
    undefined,
    false,
    3,
  );

  // The pending bound includes work waiting on a worker response
  for (let id = 0; id < 1000; id++) {
    pool.verify(1, new Uint8Array([id]), id);
  }
  expect(pool.droppedCount()).toBe(997);

  worker.completeNext(); // stale artifact 0 only frees the worker
  expect(skipped).toEqual([]);
  worker.completeNext(); // artifact 997 reports the gap
  worker.completeNext(); // artifact 998
  worker.completeNext(); // artifact 999
  expect(verifiedReceivedAt).toEqual([997, 998, 999]);
  expect(skipped).toEqual([997]);
  expect(worker.messages.map((message: any) => message.sequence)).toEqual([0, 997, 998, 999]);
  pool.terminate();
});
