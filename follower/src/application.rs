use crate::throughput::Throughput;
use alto_types::{Block, Scheme};
use commonware_consensus::{
    marshal::{self, Update},
    types::Height,
    Reporter,
};
use commonware_runtime::{spawn_cell, Clock, ContextCell, Handle, Spawner};
use commonware_utils::Acknowledgement;
use futures::{channel::mpsc, SinkExt, StreamExt};
use tracing::info;

const THROUGHPUT_WINDOW: std::time::Duration = std::time::Duration::from_secs(30);
const PRUNE_INTERVAL: u64 = 10_000;

/// Formats an estimated time of arrival (ETA) based on the remaining work and rate.
fn format_eta(remaining: u64, rate: f64) -> String {
    if remaining == 0 {
        return "0s".to_string();
    }
    if !rate.is_finite() || rate <= 0.0 {
        return "unknown".to_string();
    }

    let secs = (remaining as f64 / rate) as u64;
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if h > 0 {
        format!("{h}h{m:02}m{s:02}s")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}

/// Formats ETA when remaining work may be unknown (e.g. tip not received yet).
fn format_eta_maybe(remaining: Option<u64>, rate: f64) -> String {
    match remaining {
        Some(remaining) => format_eta(remaining, rate),
        None => "unknown".to_string(),
    }
}

/// A forwarder of [Update] messages to the [Application].
#[derive(Clone)]
pub(crate) struct Mailbox {
    tx: mpsc::Sender<Update<Block>>,
}

impl Reporter for Mailbox {
    type Activity = Update<Block>;

    async fn report(&mut self, activity: Self::Activity) {
        let _ = self.tx.send(activity).await;
    }
}

/// A simple application that tracks just tracks the rate of block processing.
pub(crate) struct Application<E: Clock + Spawner> {
    context: ContextCell<E>,
    rx: mpsc::Receiver<Update<Block>>,
    throughput: Throughput,
    tip: Option<Height>,
    mailbox: marshal::Mailbox<Scheme, Block>,
    pruning_depth: Option<u64>,
}

impl<E: Clock + Spawner> Application<E> {
    pub(crate) fn new(
        context: E,
        mailbox: marshal::Mailbox<Scheme, Block>,
        mailbox_size: usize,
        pruning_depth: Option<u64>,
    ) -> (Self, Mailbox) {
        let (tx, rx) = mpsc::channel(mailbox_size);
        let app = Self {
            context: ContextCell::new(context.clone()),
            rx,
            throughput: Throughput::new(THROUGHPUT_WINDOW),
            tip: None,
            mailbox,
            pruning_depth,
        };
        (app, Mailbox { tx })
    }

    pub(crate) fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run().await)
    }

    async fn run(mut self) {
        while let Some(msg) = self.rx.next().await {
            match msg {
                Update::Tip(_, height, _) => {
                    self.tip = Some(height);
                }
                Update::Block(block, ack) => {
                    // This is where an application would process the
                    // finalized block (e.g. update state, index transactions,
                    // serve queries, etc.).
                    let height = block.height.get();
                    let bps = self.throughput.record(self.context.current());
                    let remaining = self.tip.map(|t| t.get().saturating_sub(height));
                    info!(
                        height,
                        tip = self.tip.map(|h| h.get()),
                        bps = %format_args!("{bps:.2}"),
                        eta = %format_args!("{}", format_eta_maybe(remaining, bps)),
                        "processed block"
                    );
                    ack.acknowledge();

                    // Prune the archive if the height is a multiple of the prune interval.
                    if let Some(depth) = self.pruning_depth.filter(|_| height % PRUNE_INTERVAL == 0)
                    {
                        let prune_to = height.saturating_sub(depth);
                        if prune_to > 0 {
                            self.mailbox.prune(Height::new(prune_to)).await;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{format_eta, format_eta_maybe};

    #[test]
    fn eta_is_unknown_when_rate_is_zero_and_remaining_non_zero() {
        assert_eq!(format_eta(42, 0.0), "unknown");
    }

    #[test]
    fn eta_is_zero_when_no_remaining_work() {
        assert_eq!(format_eta(0, 0.0), "0s");
    }

    #[test]
    fn eta_is_unknown_when_remaining_is_unknown() {
        assert_eq!(format_eta_maybe(None, 123.0), "unknown");
    }
}
