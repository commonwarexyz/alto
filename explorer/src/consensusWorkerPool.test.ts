import { expect, test } from "@jest/globals";
import { ConsensusWorkerPool, consensusWorkerCount, MAX_PENDING_JOBS } from "./consensusWorkerPool";

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
    this.completed += 1;
    this.onmessage?.({ data: { artifact: null, error } } as MessageEvent);
  }
}

const workerFactory = (workers: FakeWorker[]) => () => {
  const worker = new FakeWorker();
  workers.push(worker);
  return worker as unknown as Worker;
};

test("dispatches every artifact across the verifier pool without sampling", () => {
  const workers: FakeWorker[] = [];
  const pool = new ConsensusWorkerPool(
    3,
    workerFactory(workers),
    new Uint8Array([1, 2, 3]),
    false,
    (error) => { throw error; },
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
  expect(pool.drain().map(result => result.receivedAt)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);

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
  const workers: FakeWorker[] = [];
  const delivered: number[] = [];
  const skipped: number[] = [];
  const pool = new ConsensusWorkerPool(
    2,
    workerFactory(workers),
    new Uint8Array([1]),
    false,
    (error) => { throw error; },
  );
  const [stalled, healthy] = workers;
  const total = MAX_PENDING_JOBS * 2;
  pool.verify(1, new Uint8Array([0]), 0);
  for (let id = 1; id < total; id++) {
    pool.verify(1, new Uint8Array([id]), id);
    healthy.completeNext();
    for (const result of pool.drain()) {
      if (result.skipped) skipped.push(result.skipped);
      delivered.push(result.receivedAt);
    }
  }
  expect(delivered).toEqual(Array.from({ length: total - 1 }, (_, i) => i + 1));
  expect(skipped).toEqual([1]);

  // A late response cannot resurrect work already reported as skipped.
  stalled.completeNext();
  expect(pool.drain()).toEqual([]);
  pool.terminate();
});

test("retries an in-flight artifact on a replacement worker", () => {
  const workers: FakeWorker[] = [];
  const errors: (ErrorEvent | Error)[] = [];
  const pool = new ConsensusWorkerPool(
    1,
    workerFactory(workers),
    new Uint8Array([1, 2, 3]),
    false,
    (error) => errors.push(error),
  );

  pool.verify(1, new Uint8Array([9]), 42);
  const worker = workers[0];
  worker.fail();

  expect(worker.terminated).toBe(true);
  expect(workers).toHaveLength(2);
  expect((workers[1].messages[0] as any).payload[0]).toBe(9);

  workers[1].completeNext();
  expect(pool.drain().map(result => result.receivedAt)).toEqual([42]);
  expect(errors).toEqual([]);
});

test("a failed verification still releases later results in order and replaces the worker", () => {
  const workers: FakeWorker[] = [];
  const errors: (ErrorEvent | Error)[] = [];
  const pool = new ConsensusWorkerPool(
    2,
    workerFactory(workers),
    new Uint8Array([1, 2, 3]),
    false,
    (error) => errors.push(error),
  );
  const [first, second] = workers;
  pool.verify(1, new Uint8Array([0]), 0);
  pool.verify(1, new Uint8Array([1]), 1);

  // Hold the second artifact's result until the first reports.
  second.completeNext();
  expect(pool.drain()).toEqual([]);
  first.completeNext("wasm panic");
  expect(pool.drain().map(result => result.receivedAt)).toEqual([0, 1]);

  // Retire the failed worker (its WASM may be poisoned) and refill its slot without retrying the
  // released result.
  expect(first.terminated).toBe(true);
  expect(workers).toHaveLength(3);
  expect(workers[2].messages).toHaveLength(0);
  expect(errors).toEqual([]);

  // A non-error reply resets the failure count. Repeated failures without one stop the pool.
  pool.verify(1, new Uint8Array([2]), 2);
  second.completeNext();
  expect(pool.drain().map(result => result.receivedAt)).toEqual([2]);
  for (let id = 3; id < 6; id++) {
    pool.verify(1, new Uint8Array([id]), id);
    const active = workers.find(
      (worker) => !worker.terminated && worker.messages.length > worker.completed,
    )!;
    active.completeNext("wasm init failed");
  }
  expect(errors).toHaveLength(1);
  expect(workers.every((worker) => worker.terminated)).toBe(true);
  expect(pool.drain().map(result => result.receivedAt)).toEqual([3, 4, 5]);
});

test("an idle worker that fails is removed and replaced, and repeated failures stop the pool", () => {
  const workers: FakeWorker[] = [];
  const errors: (ErrorEvent | Error)[] = [];
  const pool = new ConsensusWorkerPool(
    1,
    workerFactory(workers),
    new Uint8Array([1, 2, 3]),
    false,
    (error) => errors.push(error),
  );
  const worker = workers[0];

  // Fails before any artifact was dispatched (e.g. the worker script did not load).
  worker.fail();
  expect(worker.terminated).toBe(true);
  expect(workers).toHaveLength(2);

  // Work goes to the replacement, never to the dead worker.
  pool.verify(1, new Uint8Array([7]), 7);
  expect(worker.messages).toHaveLength(0);
  expect(workers[1].messages).toHaveLength(1);

  // Consecutive failures without progress eventually surface an error instead of respawning forever.
  workers[1].fail();
  workers[2].fail();
  expect(errors).toHaveLength(1);
  expect(workers.every((worker) => worker.terminated)).toBe(true);
});

test("bounds pending work when every worker stalls", () => {
  const workers: FakeWorker[] = [];
  const pool = new ConsensusWorkerPool(
    1,
    workerFactory(workers),
    new Uint8Array([1, 2, 3]),
    false,
    (error) => { throw error; },
  );
  const worker = workers[0];
  const total = MAX_PENDING_JOBS * 4;
  const firstRetained = total - MAX_PENDING_JOBS;

  // The retained window includes work waiting on a worker response.
  for (let id = 0; id < total; id++) {
    pool.verify(1, new Uint8Array([id]), id);
  }
  expect(pool.drain()).toEqual([]);

  worker.completeNext(); // stale artifact 0 only frees the worker
  expect(pool.drain()).toEqual([]);
  for (let id = firstRetained; id < total; id++) {
    worker.completeNext();
  }
  const retained = Array.from({ length: MAX_PENDING_JOBS }, (_, i) => firstRetained + i);
  const results = pool.drain();
  expect(results.map(result => result.receivedAt)).toEqual(retained);
  expect(results.filter(result => result.skipped).map(result => result.skipped)).toEqual([firstRetained]);
  expect(worker.messages.map((message: any) => message.payload[0])).toEqual(
    Array.from(new Uint8Array([0, ...retained])),
  );
  pool.terminate();
});

test("keeps only recent results when verification outpaces consumption", () => {
  const workers: FakeWorker[] = [];
  const pool = new ConsensusWorkerPool(
    1,
    workerFactory(workers),
    new Uint8Array([1]),
    false,
    (error) => { throw error; },
  );
  const total = MAX_PENDING_JOBS * 4;
  const firstRetained = total - MAX_PENDING_JOBS;

  // Workers keep making progress while the consumer waits between batches.
  for (let id = 0; id < total; id++) {
    pool.verify(1, new Uint8Array([id]), id);
    workers[0].completeNext();
  }

  const results = pool.drain();
  expect(results.map(result => result.receivedAt)).toEqual(
    Array.from({ length: MAX_PENDING_JOBS }, (_, i) => firstRetained + i),
  );
  expect(results.filter(result => result.skipped).map(result => result.skipped)).toEqual([firstRetained]);
  expect(pool.drain()).toEqual([]);
  pool.terminate();
});

test("terminates created workers when startup fails", () => {
  const worker = new FakeWorker();
  const failure = new Error("worker creation failed");
  let created = 0;
  const errors: (ErrorEvent | Error)[] = [];
  expect(() => new ConsensusWorkerPool(
    2,
    () => {
      if (created++ === 0) return worker as unknown as Worker;
      throw failure;
    },
    new Uint8Array([1]),
    false,
    (error) => errors.push(error),
  )).toThrow(failure);
  expect(worker.terminated).toBe(true);
  expect(errors).toEqual([]);
});
