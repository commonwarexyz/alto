import type { FinalizedJs, NotarizedJs, SeedJs } from "./types";

export type ConsensusArtifact = SeedJs | NotarizedJs | FinalizedJs;

export interface VerifiedConsensusArtifact {
  kind: number;
  artifact: ConsensusArtifact | null;
  receivedAt: number;
  /// Number of artifacts shed immediately before this one because verification fell behind. Set
  /// only on a marker result (with a `null` artifact) emitted ahead of the next verified artifact.
  skipped?: number;
}

export const consensusWorkerCount = (hardwareConcurrency: number): number =>
  Math.min(16, Math.max(2, hardwareConcurrency - 1));

/// Most artifacts waiting for a worker. When the feed outpaces verification, the oldest queued
/// artifacts are dropped so the live head of the timeline stays current instead of drifting behind
/// while memory grows.
export const MAX_QUEUED_JOBS = 256;

/// Consecutive worker failures without a successful verification before the pool gives up.
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

interface WorkerVerifiedConsensusArtifact extends VerifiedConsensusArtifact {
  sequence: number;
  error?: string;
}

export class ConsensusWorkerPool {
  private readonly availableWorkers: Worker[];
  private readonly queuedJobs: VerificationJob[] = [];
  private readonly completedResults = new Map<number, VerifiedConsensusArtifact>();
  private readonly droppedSequences = new Set<number>();
  private readonly activeJobs = new Map<Worker, VerificationJob>();
  private nextJobSequence = 0;
  private nextResultSequence = 0;
  private consecutiveFailures = 0;
  private dropped = 0;
  private pendingSkipped = 0;
  private lastDropWarning = 0;
  private stopped = false;

  constructor(
    private readonly workers: Worker[],
    private readonly publicKey: Uint8Array,
    private readonly onVerified: (result: VerifiedConsensusArtifact) => void,
    private readonly onError?: (error: ErrorEvent | Error) => void,
    private readonly createWorker?: () => Worker,
    private readonly standard = false,
    private readonly maxQueuedJobs = MAX_QUEUED_JOBS,
  ) {
    this.availableWorkers = [];
    for (const worker of workers) {
      this.configureWorker(worker);
      this.availableWorkers.push(worker);
    }
  }

  private configureWorker(worker: Worker) {
    worker.onmessage = (event: MessageEvent<WorkerVerifiedConsensusArtifact>) => {
      if (this.stopped) {
        return;
      }
      const { sequence, kind, artifact, receivedAt, error } = event.data;
      const activeJob = this.activeJobs.get(worker);
      if (!activeJob || activeJob.sequence !== sequence) {
        return;
      }
      this.activeJobs.delete(worker);
      // Release the (possibly null) result first so in-order delivery keeps advancing.
      this.completedResults.set(sequence, { kind, artifact, receivedAt });
      this.releaseCompleted();
      if (error) {
        // The worker could not verify (its wasm module failed to initialize or panicked, or the
        // artifact does not match the configured identity). Replace it so a poisoned worker does
        // not keep swallowing its share of the feed, and give up after repeated failures.
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
    worker.postMessage({ type: "initialize", publicKey: this.publicKey, standard: this.standard });
  }

  /// Hand completed results to the consumer in sequence order. Dropped artifacts are skipped and
  /// reported as a `skipped` marker ahead of the next released result so the consumer knows the
  /// stream has a gap.
  private releaseCompleted() {
    for (;;) {
      if (this.droppedSequences.delete(this.nextResultSequence)) {
        this.nextResultSequence += 1;
        this.pendingSkipped += 1;
        continue;
      }
      const result = this.completedResults.get(this.nextResultSequence);
      if (!result) {
        return;
      }
      this.completedResults.delete(this.nextResultSequence);
      this.nextResultSequence += 1;
      if (this.pendingSkipped > 0) {
        const skipped = this.pendingSkipped;
        this.pendingSkipped = 0;
        this.onVerified({ kind: result.kind, artifact: null, receivedAt: result.receivedAt, skipped });
      }
      this.onVerified(result);
    }
  }

  /// Number of artifacts dropped because verification could not keep up with the feed.
  droppedCount(): number {
    return this.dropped;
  }

  /// Retire a worker that crashed (`onerror`) or reported a verification error. An in-flight job
  /// is retried on the replacement; a job whose result was already released is not.
  private handleWorkerFailure(worker: Worker, error: ErrorEvent | Error) {
    const job = this.activeJobs.get(worker);
    this.activeJobs.delete(worker);
    // A worker can fail while idle (for example, its script failed to load); never dispatch to it.
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
      this.onError?.(error);
      this.terminate();
      return;
    }
    if (job) {
      this.queuedJobs.unshift(job);
    }

    const workerIndex = this.workers.indexOf(worker);
    if (workerIndex === -1 || !this.createWorker) {
      this.onError?.(error);
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
      this.onError?.(error);
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
    // Shed the oldest waiting artifacts when the feed outpaces verification; their sequence
    // numbers are marked dropped so in-order release keeps advancing.
    let shed = 0;
    while (this.queuedJobs.length > this.maxQueuedJobs) {
      const stale = this.queuedJobs.shift()!;
      this.droppedSequences.add(stale.sequence);
      shed += 1;
    }
    if (shed > 0) {
      this.dropped += shed;
      const now = Date.now();
      if (now - this.lastDropWarning >= DROP_WARNING_INTERVAL_MS) {
        this.lastDropWarning = now;
        console.warn(
          `consensus verification is falling behind: ${this.dropped} artifacts skipped so far`,
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
      const { sequence, kind, payload, receivedAt } = job;
      worker.postMessage({ type: "verify", sequence, kind, payload, receivedAt });
    }
  }

  terminate() {
    this.stopped = true;
    this.queuedJobs.length = 0;
    this.availableWorkers.length = 0;
    this.completedResults.clear();
    this.droppedSequences.clear();
    this.pendingSkipped = 0;
    this.activeJobs.clear();
    for (const worker of this.workers) {
      worker.terminate();
    }
  }
}
