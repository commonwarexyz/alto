use crate::Source;
use alto_client::consensus::Message;
use alto_types::{Block, Scheme};
use commonware_broadcast::Broadcaster;
use commonware_consensus::{
    marshal,
    simplex::types::Activity,
    Reporter, Viewable,
};
use commonware_cryptography::ed25519::PublicKey;
use commonware_parallel::Sequential;
use commonware_runtime::Clock;
use futures::StreamExt;
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

pub struct CertificateFeeder<E: Clock, C: Source> {
    context: E,
    client: C,
    scheme: Scheme,
    marshal_mailbox: marshal::Mailbox<Scheme, Block>,
    buffer_mailbox: commonware_broadcast::buffered::Mailbox<PublicKey, Block>,
}

impl<E: Clock, C: Source> CertificateFeeder<E, C> {
    pub fn new(
        context: E,
        client: C,
        scheme: Scheme,
        marshal_mailbox: marshal::Mailbox<Scheme, Block>,
        buffer_mailbox: commonware_broadcast::buffered::Mailbox<PublicKey, Block>,
    ) -> Self {
        Self {
            context,
            client,
            scheme,
            marshal_mailbox,
            buffer_mailbox,
        }
    }

    pub async fn run(mut self) {
        loop {
            if let Err(e) = self.process_stream().await {
                error!(error = ?e, "stream error");
            }
            self.context.sleep(Duration::from_secs(1)).await;
        }
    }

    async fn process_stream(&mut self) -> Result<(), String> {
        let client = self.client.clone();
        let mut stream = client
            .listen()
            .await
            .map_err(|e| format!("failed to connect: {e}"))?;

        info!("connected to certificate stream");

        while let Some(result) = stream.next().await {
            let message = result.map_err(|e| format!("stream error: {e}"))?;
            self.handle_message(message).await?;
        }

        warn!("certificate stream disconnected, reconnecting...");
        Ok(())
    }

    async fn handle_message(&mut self, message: Message) -> Result<(), String> {
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
                    return Err(format!(
                        "invalid finalization signature for height {}",
                        height.get()
                    ));
                }

                let no_peers = commonware_p2p::Recipients::Some(vec![]);
                _ = self
                    .buffer_mailbox
                    .broadcast(no_peers, finalized.block.clone())
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
                trace!(
                    height = notarized.block.height.get(),
                    view = notarized.proof.view().get(),
                    "received notarization (ignored)"
                );
            }
            Message::Seed(seed) => {
                trace!(view = seed.view().get(), "received seed (ignored)");
            }
        }
        Ok(())
    }
}
