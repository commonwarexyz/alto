import type { FinalizedJs, NotarizedJs, SeedJs } from "./types";

export type ConsensusArtifact = SeedJs | NotarizedJs | FinalizedJs;

export interface VerifiedConsensusArtifact {
  kind: number;
  artifact: ConsensusArtifact | null;
  receivedAt: number;
}

export const consensusWorkerCount = (hardwareConcurrency: number): number =>
  Math.min(16, Math.max(2, hardwareConcurrency - 1));

interface VerificationJob {
  sequence: number;
  kind: number;
  payload: Uint8Array;
  receivedAt: number;
  attempts: number;
}

interface WorkerVerifiedConsensusArtifact extends VerifiedConsensusArtifact {
  sequence: number;
}

export class ConsensusWorkerPool {
  private readonly availableWorkers: Worker[];
  private readonly queuedJobs: VerificationJob[] = [];
  private readonly completedResults = new Map<number, VerifiedConsensusArtifact>();
  private readonly activeJobs = new Map<Worker, VerificationJob>();
  private nextJobSequence = 0;
  private nextResultSequence = 0;
  private stopped = false;

  constructor(
    private readonly workers: Worker[],
    private readonly publicKey: Uint8Array,
    private readonly onVerified: (result: VerifiedConsensusArtifact) => void,
    private readonly onError?: (event: ErrorEvent) => void,
    private readonly createWorker?: () => Worker,
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
      const { sequence, kind, artifact, receivedAt } = event.data;
      const activeJob = this.activeJobs.get(worker);
      if (!activeJob || activeJob.sequence !== sequence) {
        return;
      }
      this.activeJobs.delete(worker);
      this.completedResults.set(sequence, { kind, artifact, receivedAt });
      while (this.completedResults.has(this.nextResultSequence)) {
        this.onVerified(this.completedResults.get(this.nextResultSequence)!);
        this.completedResults.delete(this.nextResultSequence);
        this.nextResultSequence += 1;
      }
      this.availableWorkers.push(worker);
      this.dispatch();
    };
    worker.onerror = (event) => this.handleWorkerError(worker, event);
    worker.postMessage({ type: "initialize", publicKey: this.publicKey });
  }

  private handleWorkerError(worker: Worker, event: ErrorEvent) {
    event.preventDefault();
    const job = this.activeJobs.get(worker);
    this.activeJobs.delete(worker);
    worker.terminate();

    if (this.stopped) {
      return;
    }
    if (job && job.attempts >= 3) {
      this.onError?.(event);
      this.terminate();
      return;
    }
    if (job) {
      this.queuedJobs.unshift(job);
    }

    const workerIndex = this.workers.indexOf(worker);
    if (workerIndex === -1 || !this.createWorker) {
      this.onError?.(event);
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
      this.onError?.(event);
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
    this.activeJobs.clear();
    for (const worker of this.workers) {
      worker.terminate();
    }
  }
}
