use crate::engine::{BlockArchive, FinalizedArchive};
use crate::Source;
use alto_client::IndexQuery;
use alto_types::{Finalized, Scheme};
use commonware_consensus::simplex::types::Subject;
use commonware_consensus::types::Height;
use commonware_cryptography::certificate::Scheme as CertScheme;
use commonware_cryptography::{sha256, Committable};
use commonware_parallel::{Rayon, Strategy};
use commonware_runtime::{Metrics, Spawner, Storage, ThreadPooler};
use commonware_storage::archive::Archive;
use commonware_utils::channel::mpsc;
use commonware_utils::N3f1;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use governor::clock::Clock as GClock;
use rand::rngs::OsRng;
use rand::{CryptoRng, Rng};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::time::Instant;
use tracing::{info, warn};

const SYNC_INTERVAL: u64 = 16_384;
const LOG_INTERVAL: u64 = 1000;
const BATCH_SIZE: usize = 1024;

type FetchResult<E> = (u64, Result<Finalized, E>);
type BoxFetch<E> = Pin<Box<dyn Future<Output = FetchResult<E>> + Send>>;

/// Fills the archives with finalized blocks from a [Source] before marshal starts.
///
/// Uses a two-stage pipeline:
///   1. Fetch loop: concurrent HTTP fetches via [FuturesUnordered], accumulates
///      batches, and sends full batches to the writer task.
///   2. Writer task: receives batches, batch-verifies signatures using
///      `Scheme::verify_certificates()` (MSM-based), and writes to archives.
///
/// Not all heights will have blocks (skipped views are normal). Failed fetches
/// are skipped -- marshal will handle any remaining gaps during live following.
pub struct Backfiller<C, E>
where
    C: Source,
    E: commonware_runtime::Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics + ThreadPooler,
{
    context: E,
    client: C,
    scheme: Scheme,
    strategy: Rayon,
    finalizations_by_height: FinalizedArchive<E>,
    finalized_blocks: BlockArchive<E>,
    tip: Height,
    concurrency: usize,
}

impl<C, E> Backfiller<C, E>
where
    C: Source,
    E: commonware_runtime::Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics + ThreadPooler,
{
    pub fn new(
        context: E,
        client: C,
        scheme: Scheme,
        finalizations_by_height: FinalizedArchive<E>,
        finalized_blocks: BlockArchive<E>,
        tip: Height,
        concurrency: usize,
        worker_threads: usize,
    ) -> Self {
        let strategy = context
            .create_strategy(NonZeroUsize::new(worker_threads).expect("worker_threads must be non-zero"))
            .expect("failed to create thread pool");
        Self {
            context,
            client,
            scheme,
            strategy,
            finalizations_by_height,
            finalized_blocks,
            tip,
            concurrency,
        }
    }

    /// Run the backfiller to completion, returning the filled archives.
    pub async fn run(self) -> (FinalizedArchive<E>, BlockArchive<E>) {
        let Self {
            context,
            client,
            scheme,
            strategy,
            finalizations_by_height,
            finalized_blocks,
            tip,
            concurrency,
        } = self;
        let tip = tip.get();

        let start = finalized_blocks.last_index().map_or(1, |i| i + 1);
        if start > tip {
            info!(tip, "archives already caught up, skipping backfill");
            return (finalizations_by_height, finalized_blocks);
        }

        let total = tip - start + 1;
        info!(start, tip, total, "starting backfill");

        // Channel connecting fetch loop to writer task (buffer 2 batches)
        let (batch_tx, mut batch_rx) = mpsc::channel::<Vec<Finalized>>(2);

        // Spawn writer task (owns archives, verifies + writes batches)
        let mut finalizations_by_height = finalizations_by_height;
        let mut finalized_blocks = finalized_blocks;
        let writer_scheme = scheme;
        let writer_strategy = strategy;

        let writer_handle = context.clone().spawn(move |_| async move {
            let mut total_written = 0u64;
            let mut last_log_time = Instant::now();
            let mut last_log_written = 0u64;
            let mut pending_writes = 0u64;

            while let Some(batch) = batch_rx.recv().await {
                // Pre-warm all Lazy<Signature> in parallel (G1 decompression + subgroup check)
                writer_strategy.map_collect_vec(batch.iter(), |f| f.proof.certificate.get().is_some());

                let valid = writer_scheme
                    .verify_certificates::<_, sha256::Digest, _, N3f1>(
                        &mut OsRng,
                        batch.iter().map(|f| {
                            (
                                Subject::Finalize {
                                    proposal: &f.proof.proposal,
                                },
                                &f.proof.certificate,
                            )
                        }),
                        &writer_strategy,
                    );
                assert!(valid, "batch verification failed");

                let count = batch.len() as u64;
                for finalized in batch {
                    let height = finalized.block.height.get();
                    let commitment = finalized.block.commitment();
                    finalized_blocks
                        .put(height, commitment, finalized.block)
                        .await
                        .expect("failed to write block");
                    finalizations_by_height
                        .put(height, commitment, finalized.proof)
                        .await
                        .expect("failed to write finalization");
                }

                total_written += count;
                pending_writes += count;
                if pending_writes >= SYNC_INTERVAL {
                    finalized_blocks
                        .sync()
                        .await
                        .expect("failed to sync blocks");
                    finalizations_by_height
                        .sync()
                        .await
                        .expect("failed to sync finalizations");
                    pending_writes = 0;
                }

                if total_written - last_log_written >= LOG_INTERVAL {
                    let elapsed = last_log_time.elapsed();
                    let bps =
                        (total_written - last_log_written) as f64 / elapsed.as_secs_f64();
                    let remaining = tip.saturating_sub(start + total_written - 1);
                    let eta_secs =
                        if bps > 0.0 { (remaining as f64 / bps) as u64 } else { 0 };
                    let hours = eta_secs / 3600;
                    let minutes = (eta_secs % 3600) / 60;
                    let seconds = eta_secs % 60;
                    info!(
                        written = total_written,
                        remaining,
                        blocks_per_sec = format!("{bps:.1}"),
                        eta = format!("{hours}h{minutes:02}m{seconds:02}s"),
                        "backfill progress"
                    );
                    last_log_written = total_written;
                    last_log_time = Instant::now();
                }
            }

            // Final sync
            if pending_writes > 0 {
                finalized_blocks
                    .sync()
                    .await
                    .expect("failed to sync blocks");
                finalizations_by_height
                    .sync()
                    .await
                    .expect("failed to sync finalizations");
            }

            info!(tip, total_written, "backfill complete");
            (finalizations_by_height, finalized_blocks)
        });

        // Fetch loop: accumulate batches and send to writer
        let fetch = |height: u64| -> BoxFetch<C::Error> {
            let client = client.clone();
            Box::pin(async move {
                (height, client.finalized_unverified(IndexQuery::Index(height)).await)
            })
        };

        let mut batch: Vec<Finalized> = Vec::with_capacity(BATCH_SIZE);
        let mut futures: FuturesUnordered<BoxFetch<C::Error>> = FuturesUnordered::new();
        let mut next_height = start;

        // Seed the pipeline
        while next_height <= tip && futures.len() < concurrency {
            futures.push(fetch(next_height));
            next_height += 1;
        }

        while let Some((height, result)) = futures.next().await {
            match result {
                Ok(finalized) => {
                    batch.push(finalized);
                    if batch.len() >= BATCH_SIZE {
                        let full_batch =
                            std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                        batch_tx.send(full_batch).await.expect("writer task died");
                    }
                }
                Err(e) => {
                    warn!(height, error = ?e, "failed to fetch block, skipping");
                }
            }

            if next_height <= tip {
                futures.push(fetch(next_height));
                next_height += 1;
            }
        }

        // Send remaining batch
        if !batch.is_empty() {
            batch_tx.send(batch).await.expect("writer task died");
        }

        // Close channel and wait for writer to finish
        drop(batch_tx);
        writer_handle.await.expect("writer task panicked")
    }
}
