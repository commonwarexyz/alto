use crate::Source;
use alto_client::consensus::Message;
use alto_types::{Block, Scheme};
use commonware_consensus::{marshal, simplex::types::Activity, Reporter, Viewable};
use commonware_parallel::Sequential;
use commonware_runtime::{spawn_cell, Clock, ContextCell, Handle, Spawner};
use futures::StreamExt;
use std::{fmt, time::Duration};
use tracing::{debug, error, info, trace, warn};

#[derive(Debug)]
pub enum FeederError {
    Connect(String),
    Stream(String),
    InvalidSignature { height: u64 },
}

impl fmt::Display for FeederError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(e) => write!(f, "failed to connect: {e}"),
            Self::Stream(e) => write!(f, "stream error: {e}"),
            Self::InvalidSignature { height } => {
                write!(f, "invalid finalization signature for height {height}")
            }
        }
    }
}

impl std::error::Error for FeederError {}

pub struct CertificateFeeder<E: Clock, C: Source> {
    context: ContextCell<E>,
    client: C,
    scheme: Scheme,
    marshal_mailbox: marshal::Mailbox<Scheme, Block>,
}

impl<E: Clock + Spawner, C: Source> CertificateFeeder<E, C> {
    pub fn new(
        context: E,
        client: C,
        scheme: Scheme,
        marshal_mailbox: marshal::Mailbox<Scheme, Block>,
    ) -> Self {
        Self {
            context: ContextCell::new(context),
            client,
            scheme,
            marshal_mailbox,
        }
    }

    pub fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run().await)
    }

    pub async fn run(mut self) {
        loop {
            if let Err(e) = self.process_stream().await {
                error!(error = %e, "stream error");
            }
            self.context.sleep(Duration::from_secs(1)).await;
        }
    }

    async fn process_stream(&mut self) -> Result<(), FeederError> {
        let client = self.client.clone();
        let mut stream = client
            .listen()
            .await
            .map_err(|e| FeederError::Connect(e.to_string()))?;

        info!("connected to certificate stream");

        while let Some(result) = stream.next().await {
            let message = result.map_err(|e| FeederError::Stream(e.to_string()))?;
            self.handle_message(message).await?;
        }

        warn!("certificate stream disconnected, reconnecting...");
        Ok(())
    }

    pub(crate) async fn handle_message(&mut self, message: Message) -> Result<(), FeederError> {
        match message {
            Message::Finalization(finalized) => {
                let height = finalized.block.height;
                let view = finalized.proof.view();

                debug!(
                    height = height.get(),
                    view = view.get(),
                    "received finalization"
                );

                if !finalized.verify(&self.scheme, &Sequential) {
                    return Err(FeederError::InvalidSignature {
                        height: height.get(),
                    });
                }

                let round = finalized.proof.round();
                self.marshal_mailbox
                    .verified(round, finalized.block.clone())
                    .await;
                self.marshal_mailbox
                    .report(Activity::Finalization(finalized.proof.clone()))
                    .await;

                info!(
                    height = height.get(),
                    view = view.get(),
                    "reported finalization"
                );
            }
            Message::Notarization(notarized) => {
                let round = notarized.proof.round();
                debug!(
                    height = notarized.block.height.get(),
                    view = round.view().get(),
                    "received notarization"
                );

                if !notarized.verify(&self.scheme, &Sequential) {
                    return Err(FeederError::InvalidSignature {
                        height: notarized.block.height.get(),
                    });
                }

                // This block may not actually be verified (we would only know that once certified). However, it does no damage
                // to store it in marshal before a finalization arrives (if a block isn't directly finalized, this will prevent us from
                // having to ask the backend for it again).
                //
                // If it is invalid and not part of the canonical chain, we'll just prune it later.
                self.marshal_mailbox
                    .verified(round, notarized.block.clone())
                    .await;
                self.marshal_mailbox
                    .report(Activity::Notarization(notarized.proof.clone()))
                    .await;

                info!(
                    height = notarized.block.height.get(),
                    view = round.view().get(),
                    "reported notarization"
                );
            }
            Message::Seed(seed) => {
                trace!(view = seed.view().get(), "received seed");
            }
        }
        Ok(())
    }
}
