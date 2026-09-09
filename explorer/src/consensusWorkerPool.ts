import type { CertifiedBlockJs, SeedJs } from "./types";

export type ConsensusArtifact = SeedJs | CertifiedBlockJs;

export interface VerifiedConsensusArtifact {
  kind: number;
  artifact: ConsensusArtifact | null;
  receivedAt: number;
  /// Number of artifacts shed immediately before this one because processing fell behind.
  skipped?: number;
}

export const consensusWorkerCount = (hardwareConcurrency: number): number =>
  Math.min(16, Math.max(2, hardwareConcurrency - 1));

/// Maximum number of artifacts retained for consumption.
/// Older work is shed to keep the timeline current. Active jobs may finish after being shed.
export const MAX_PENDING_JOBS = 256;

/// Consecutive worker failures before stopping. Any non-error reply resets the count.
const MAX_CONSECUTIVE_FAILURES = 3;

/// Minimum interval between warnings that artifacts are being shed.
const DROP_WARNING_INTERVAL_MS = 5_000;

interface VerificationJob {
  sequence: number;
  kind: number;
  payload: Uint8Array;
  receivedAt: number;
  attempts: number;
}

export class ConsensusWorkerPool {
  private readonly workers: Worker[] = [];
  private readonly availableWorkers: Worker[] = [];
  private readonly queuedJobs: VerificationJob[] = [];
  private readonly completedResults = new Map<number, VerifiedConsensusArtifact>();
  private readonly activeJobs = new Map<Worker, VerificationJob>();
  private nextJobSequence = 0;
  private nextResultSequence = 0;
  private consecutiveFailures = 0;
  private dropped = 0;
  private pendingSkipped = 0;
  private lastDropWarning = 0;
  private stopped = false;

  constructor(
    workerCount: number,
    private readonly createWorker: () => Worker,
    private readonly publicKey: Uint8Array,
    private readonly standard: boolean,
    private readonly onError: (error: ErrorEvent | Error) => void,
  ) {
    try {
      for (let index = 0; index < workerCount; index++) {
        const worker = this.createWorker();
        this.workers.push(worker);
        this.configureWorker(worker);
        this.availableWorkers.push(worker);
      }
    } catch (error) {
      this.terminate();
      throw error;
    }
  }

  private configureWorker(worker: Worker) {
    worker.onmessage = (
      event: MessageEvent<{ artifact: ConsensusArtifact | null; error?: string }>,
    ) => {
      // Each worker owns one active job until it replies or is retired.
      const activeJob = this.activeJobs.get(worker);
      if (!activeJob) {
        return;
      }

      // A reply completes the job even when verification failed.
      const { artifact, error } = event.data;
      const { sequence } = activeJob;
      this.activeJobs.delete(worker);
      if (sequence >= this.nextResultSequence) {
        this.completedResults.set(sequence, {
          kind: activeJob.kind,
          artifact,
          receivedAt: activeJob.receivedAt,
        });
      }

      // Invalid artifacts return null without an error. Initialization failures and panics
      // require a replacement worker. Repeated failures stop the pool.
      if (error) {
        console.error(`consensus artifact verification failed: ${error}`);
        this.handleWorkerFailure(worker, new Error(error));
        return;
      }
      this.consecutiveFailures = 0;
      this.availableWorkers.push(worker);
      this.dispatch();
    };
    worker.onerror = (event) => {
      event.preventDefault();
      this.handleWorkerFailure(worker, event);
    };
  }

  /// Consume the completed prefix in sequence order, including results ready before termination.
  /// Dropped artifacts are counted in `skipped` on the next result so the consumer knows about gaps.
  drain(): VerifiedConsensusArtifact[] {
    const results: VerifiedConsensusArtifact[] = [];
    for (;;) {
      const result = this.completedResults.get(this.nextResultSequence);
      if (!result) {
        return results;
      }
      this.completedResults.delete(this.nextResultSequence);
      this.nextResultSequence += 1;
      const skipped = this.pendingSkipped;
      this.pendingSkipped = 0;
      results.push(skipped ? { ...result, skipped } : result);
    }
  }

  /// Retire a worker that crashed (`onerror`) or reported a verification error. A retained
  /// in-flight job may be retried on the replacement. A job whose worker replied is complete.
  private handleWorkerFailure(worker: Worker, error: ErrorEvent | Error) {
    // Retire the worker from both active and idle bookkeeping before replacing it.
    const activeJob = this.activeJobs.get(worker);
    const job = activeJob && activeJob.sequence >= this.nextResultSequence ? activeJob : undefined;
    this.activeJobs.delete(worker);
    const availableIndex = this.availableWorkers.indexOf(worker);
    if (availableIndex !== -1) {
      this.availableWorkers.splice(availableIndex, 1);
    }
    worker.terminate();

    if (this.stopped) {
      return;
    }
    this.consecutiveFailures += 1;
    if ((job && job.attempts >= 3) || this.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
      this.onError(error);
      this.terminate();
      return;
    }
    if (job) {
      this.queuedJobs.unshift(job);
    }

    const workerIndex = this.workers.indexOf(worker);
    if (workerIndex === -1) {
      this.onError(error);
      this.terminate();
      return;
    }

    try {
      const replacement = this.createWorker();
      this.workers[workerIndex] = replacement;
      this.configureWorker(replacement);
      this.availableWorkers.push(replacement);
      this.dispatch();
    } catch {
      this.onError(error);
      this.terminate();
    }
  }

  verify(kind: number, payload: Uint8Array, receivedAt: number) {
    if (this.stopped) {
      return;
    }
    this.queuedJobs.push({
      sequence: this.nextJobSequence,
      kind,
      payload,
      receivedAt,
      attempts: 0,
    });
    this.nextJobSequence += 1;
    // Advance past the oldest outstanding work even if a worker has not replied. Its eventual
    // response can free that worker, but cannot reintroduce an artifact already reported as shed.
    const floor = Math.max(this.nextResultSequence, this.nextJobSequence - MAX_PENDING_JOBS);
    const shed = floor - this.nextResultSequence;
    if (shed > 0) {
      this.nextResultSequence = floor;
      this.pendingSkipped += shed;
      while (this.queuedJobs.length > 0 && this.queuedJobs[0].sequence < floor) {
        this.queuedJobs.shift();
      }
      this.completedResults.forEach((_, sequence) => {
        if (sequence < floor) this.completedResults.delete(sequence);
      });
      this.dropped += shed;
      const now = Date.now();
      if (now - this.lastDropWarning >= DROP_WARNING_INTERVAL_MS) {
        this.lastDropWarning = now;
        console.warn(
          `consensus processing is falling behind: ${this.dropped} artifacts skipped so far`,
        );
      }
    }
    this.dispatch();
  }

  private dispatch() {
    while (this.availableWorkers.length > 0 && this.queuedJobs.length > 0) {
      const worker = this.availableWorkers.shift()!;
      const job = this.queuedJobs.shift()!;
      job.attempts += 1;
      this.activeJobs.set(worker, job);
      const { kind, payload } = job;
      worker.postMessage({ kind, payload, publicKey: this.publicKey, standard: this.standard });
    }
  }

  /// Stop verification while leaving completed results available for the consumer to drain.
  terminate() {
    this.stopped = true;
    this.queuedJobs.length = 0;
    this.availableWorkers.length = 0;
    this.activeJobs.clear();
    for (const worker of this.workers) {
      worker.terminate();
    }
  }
}
