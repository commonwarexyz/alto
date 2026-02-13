use crate::engine::{BlockArchive, FinalizedArchive};
use crate::Source;
use alto_client::IndexQuery;
use alto_types::Finalized;
use commonware_consensus::types::Height;
use commonware_cryptography::Committable;
use commonware_runtime::{Metrics, Spawner, Storage};
use commonware_storage::archive::Archive;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use governor::clock::Clock as GClock;
use rand::{CryptoRng, Rng};
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tracing::{info, warn};

const SYNC_INTERVAL: u64 = 1024;
const LOG_INTERVAL: u64 = 1000;

type FetchResult<E> = (u64, Result<Finalized, E>);
type BoxFetch<E> = Pin<Box<dyn Future<Output = FetchResult<E>> + Send>>;

/// Fills the archives with finalized blocks from a [Source] before marshal starts.
///
/// Fetches blocks concurrently (the client verifies signatures on each fetch),
/// writes directly to the archives with periodic sync, and returns the filled
/// archives once caught up to `tip`.
///
/// Not all heights will have blocks (skipped views are normal). Failed fetches
/// are skipped -- marshal will handle any remaining gaps during live following.
pub struct Backfiller<C, E>
where
    C: Source,
    E: commonware_runtime::Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
{
    client: C,
    finalizations_by_height: FinalizedArchive<E>,
    finalized_blocks: BlockArchive<E>,
    tip: Height,
    concurrency: usize,
}

impl<C, E> Backfiller<C, E>
where
    C: Source,
    E: commonware_runtime::Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
{
    pub fn new(
        client: C,
        finalizations_by_height: FinalizedArchive<E>,
        finalized_blocks: BlockArchive<E>,
        tip: Height,
        concurrency: usize,
    ) -> Self {
        Self {
            client,
            finalizations_by_height,
            finalized_blocks,
            tip,
            concurrency,
        }
    }

    fn fetch(&self, height: u64) -> BoxFetch<C::Error> {
        let client = self.client.clone();
        Box::pin(async move { (height, client.finalized(IndexQuery::Index(height)).await) })
    }

    /// Run the backfiller to completion, returning the filled archives.
    pub async fn run(mut self) -> (FinalizedArchive<E>, BlockArchive<E>) {
        let tip = self.tip.get();

        let start = self.finalized_blocks.last_index().map_or(1, |i| i + 1);
        if start > tip {
            info!(tip, "archives already caught up, skipping backfill");
            return (self.finalizations_by_height, self.finalized_blocks);
        }

        let total = tip - start + 1;
        info!(start, tip, total, "starting backfill");

        let mut total_written = 0u64;
        let mut last_log_time = Instant::now();
        let mut last_log_written = 0u64;
        let mut pending_writes = 0u64;

        let mut futures: FuturesUnordered<BoxFetch<C::Error>> = FuturesUnordered::new();
        let mut next_height = start;

        // Seed the pipeline
        while next_height <= tip && futures.len() < self.concurrency {
            futures.push(self.fetch(next_height));
            next_height += 1;
        }

        while let Some((height, result)) = futures.next().await {
            match result {
                Ok(finalized) => {
                    let commitment = finalized.block.commitment();
                    self.finalized_blocks
                        .put(height, commitment, finalized.block)
                        .await
                        .expect("failed to write block");
                    self.finalizations_by_height
                        .put(height, commitment, finalized.proof)
                        .await
                        .expect("failed to write finalization");

                    total_written += 1;
                    pending_writes += 1;

                    if pending_writes >= SYNC_INTERVAL {
                        self.finalized_blocks
                            .sync()
                            .await
                            .expect("failed to sync blocks");
                        self.finalizations_by_height
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
                Err(e) => {
                    warn!(height, error = ?e, "failed to fetch block, skipping");
                }
            }

            // Spawn next fetch
            if next_height <= tip {
                futures.push(self.fetch(next_height));
                next_height += 1;
            }
        }

        // Final sync
        if pending_writes > 0 {
            self.finalized_blocks
                .sync()
                .await
                .expect("failed to sync blocks");
            self.finalizations_by_height
                .sync()
                .await
                .expect("failed to sync finalizations");
        }

        info!(tip, total_written, "backfill complete");
        (self.finalizations_by_height, self.finalized_blocks)
    }
}
