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

  failNext(error: string) {
    const message = this.messages.slice(1)[this.completed] as any;
    this.completed += 1;
    this.onmessage?.({
      data: {
        sequence: message.sequence,
        kind: message.kind,
        artifact: null,
        receivedAt: message.receivedAt,
        error,
      },
    } as MessageEvent);
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
  expect((replacements[0].messages[1] as any).payload[0]).toBe(9);

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

  // The second artifact finishes first; nothing is released until the first reports.
  second.completeNext();
  expect(verifiedReceivedAt).toEqual([]);
  first.failNext("wasm panic");
  expect(verifiedReceivedAt).toEqual([0, 1]);

  // The failing worker is retired (its wasm may be poisoned) and its slot refilled; the released
  // result is not retried.
  expect(first.terminated).toBe(true);
  expect(replacements).toHaveLength(1);
  expect(replacements[0].messages.slice(1)).toHaveLength(0);
  expect(errors).toEqual([]);

  // A successful verification resets the failure count; repeated failures without one stop the pool.
  pool.verify(1, new Uint8Array([2]), 2);
  second.completeNext();
  expect(verifiedReceivedAt).toEqual([0, 1, 2]);
  for (let id = 3; id < 6; id++) {
    pool.verify(1, new Uint8Array([id]), id);
    const active = [second, ...replacements].find(
      (worker) => !worker.terminated && worker.messages.slice(1).length > worker.completed,
    )!;
    active.failNext("wasm init failed");
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
  expect(worker.messages.slice(1)).toHaveLength(0);
  expect(replacements[0].messages.slice(1)).toHaveLength(1);

  // Consecutive failures without progress eventually surface an error instead of respawning forever.
  replacements[0].fail();
  replacements[1].fail();
  expect(errors).toHaveLength(1);
  expect(replacements.every((replacement) => replacement.terminated)).toBe(true);
});

test("sheds the oldest queued artifacts when verification falls behind", () => {
  const worker = new FakeWorker();
  const verifiedReceivedAt: number[] = [];
  const skipped: number[] = [];
  const pool = new ConsensusWorkerPool(
    [worker] as unknown as Worker[],
    new Uint8Array([1, 2, 3]),
    (result) => {
      if (result.skipped) {
        skipped.push(result.skipped);
        expect(result.artifact).toBeNull();
        return;
      }
      verifiedReceivedAt.push(result.receivedAt);
    },
    undefined,
    undefined,
    false,
    2,
  );

  // One in flight, then more than the queue bound waiting.
  for (let id = 0; id < 5; id++) {
    pool.verify(1, new Uint8Array([id]), id);
  }
  expect(pool.droppedCount()).toBe(2);

  worker.completeNext(); // artifact 0
  expect(skipped).toEqual([]);
  worker.completeNext(); // artifact 3 (1 and 2 were shed): a gap marker precedes it
  expect(skipped).toEqual([2]);
  worker.completeNext(); // artifact 4
  expect(verifiedReceivedAt).toEqual([0, 3, 4]);
  expect(skipped).toEqual([2]);
  expect(worker.messages.slice(1).map((message: any) => message.receivedAt)).toEqual([0, 3, 4]);
  pool.terminate();
});
