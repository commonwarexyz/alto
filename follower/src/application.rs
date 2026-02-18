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
const MAILBOX_SIZE: usize = 1024;

fn format_eta(remaining: u64, rate: f64) -> String {
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

enum Message {
    Tip(Height),
    Block(Block),
}

/// Thin [Reporter] that acknowledges blocks immediately and forwards
/// them to the [Application] actor for async processing.
#[derive(Clone)]
pub(crate) struct AppReporter {
    tx: mpsc::Sender<Message>,
}

impl Reporter for AppReporter {
    type Activity = Update<Block>;

    async fn report(&mut self, activity: Self::Activity) {
        match activity {
            Update::Tip(_, height, _) => {
                self.tx.send(Message::Tip(height)).await.ok();
            }
            Update::Block(block, ack) => {
                ack.acknowledge();
                self.tx.send(Message::Block(block)).await.ok();
            }
        }
    }
}

/// Application actor that processes finalized blocks on its own task,
/// decoupled from marshal's acknowledgement loop.
pub(crate) struct Application<E: Clock + Spawner> {
    context: ContextCell<E>,
    rx: mpsc::Receiver<Message>,
    throughput: Throughput,
    tip: Option<Height>,
    mailbox: marshal::Mailbox<Scheme, Block>,
    pruning_depth: Option<u64>,
}

impl<E: Clock + Spawner> Application<E> {
    pub(crate) fn new(
        context: E,
        mailbox: marshal::Mailbox<Scheme, Block>,
        pruning_depth: Option<u64>,
    ) -> (Self, AppReporter) {
        let (tx, rx) = mpsc::channel(MAILBOX_SIZE);
        let app = Self {
            context: ContextCell::new(context.clone()),
            rx,
            throughput: Throughput::new(THROUGHPUT_WINDOW),
            tip: None,
            mailbox,
            pruning_depth,
        };
        (app, AppReporter { tx })
    }

    pub(crate) fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run().await)
    }

    async fn run(mut self) {
        while let Some(msg) = self.rx.next().await {
            match msg {
                Message::Tip(height) => {
                    self.tip = Some(height);
                }
                Message::Block(block) => {
                    let height = block.height.get();
                    let bps = self.throughput.record(self.context.current());
                    let remaining = self.tip.map(|t| t.get().saturating_sub(height));
                    info!(
                        height,
                        tip = self.tip.map(|h| h.get()),
                        bps = %format_args!("{bps:.2}"),
                        eta = %format_args!("{}", format_eta(remaining.unwrap_or(0), bps)),
                        "processed block"
                    );

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
