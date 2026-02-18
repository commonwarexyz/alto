use crate::Source;
use alto_client::consensus::Message;
use alto_types::{Block, Scheme};
use commonware_consensus::{marshal, simplex::types::Activity, Reporter, Viewable};
use commonware_parallel::Sequential;
use commonware_runtime::{spawn_cell, Clock, ContextCell, Handle, Spawner};
use futures::StreamExt;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info, trace, warn};

/// Errors that can occur while feeding certificates from the source stream.
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to connect: {0}")]
    Connect(String),
    #[error("stream error: {0}")]
    Stream(String),
}

/// Feeds certificates from a [Source] stream into [marshal::Actor] via its [marshal::Mailbox].
///
/// Listens for seed, notarization, and finalization messages, verifies their threshold
/// signatures, caches the associated blocks, and reports the proofs to marshal.
/// Automatically reconnects on stream disconnection.
///
/// This is the sole signature verification point for the WebSocket streaming
/// path. The [Source] (client) is constructed without verification to avoid
/// redundant checks.
pub struct Feeder<E: Clock, C: Source> {
    context: ContextCell<E>,
    client: C,
    scheme: Scheme,
    marshal_mailbox: marshal::Mailbox<Scheme, Block>,
}

impl<E: Clock + Spawner, C: Source> Feeder<E, C> {
    /// Create a new [Feeder].
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

    /// Start the [Feeder] in a background task.
    pub fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run().await)
    }

    /// Run the feeder loop, reconnecting on stream disconnection.
    async fn run(mut self) {
        loop {
            if let Err(e) = self.process_stream().await {
                error!(error = %e, "stream error");
            }

            // Wait before reconnecting to avoid tight retry loops
            self.context.sleep(Duration::from_secs(1)).await;
        }
    }

    /// Connect to the certificate stream and process messages until
    /// the stream ends or an error occurs.
    async fn process_stream(&mut self) -> Result<(), Error> {
        // Establish a new WebSocket connection to the certificate source
        let client = self.client.clone();
        let mut stream = client
            .listen()
            .await
            .map_err(|e| Error::Connect(e.to_string()))?;
        info!("connected to certificate stream");

        // Process messages until the stream ends or an error occurs
        while let Some(result) = stream.next().await {
            let message = result.map_err(|e| Error::Stream(e.to_string()))?;
            self.handle_message(message).await?;
        }

        // Stream ended cleanly (server closed connection)
        warn!("certificate stream disconnected, reconnecting...");
        Ok(())
    }

    /// Process a single message from the certificate stream.
    ///
    /// Seed messages are ignored. Notarization and finalization messages
    /// have their threshold signatures verified before being reported to
    /// marshal along with their associated blocks.
    async fn handle_message(&mut self, message: Message) -> Result<(), Error> {
        match message {
            Message::Seed(seed) => {
                trace!(view = seed.view().get(), "received seed");
            }
            Message::Notarization(notarized) => {
                let round = notarized.proof.round();

                // Verify threshold signature (panics on invalid)
                assert!(
                    notarized.verify(&self.scheme, &Sequential),
                    "invalid notarization signature for height {}",
                    notarized.block.height.get(),
                );

                // Cache the block and report the notarization proof to marshal.
                //
                // This block may not actually be verified (we would only know
                // that once certified). However, it does no damage to store it
                // in marshal before a finalization arrives (if a block isn't
                // directly finalized, this will prevent us from having to ask
                // the backend for it again).
                //
                // If it is invalid and not part of the canonical chain, we'll
                // just prune it later.
                //
                // TODO (https://github.com/commonwarexyz/monorepo/pull/2208): create a dedicated cache for storing unverified (but notarized) blocks
                self.marshal_mailbox
                    .verified(round, notarized.block.clone())
                    .await;
                self.marshal_mailbox
                    .report(Activity::Notarization(notarized.proof.clone()))
                    .await;
                debug!(
                    height = notarized.block.height.get(),
                    view = round.view().get(),
                    "received notarization"
                );
            }
            Message::Finalization(finalized) => {
                let height = finalized.block.height;
                let view = finalized.proof.view();

                // Verify threshold signature (panics on invalid)
                assert!(
                    finalized.verify(&self.scheme, &Sequential),
                    "invalid finalization signature for height {}",
                    height.get(),
                );

                // Cache the block and report the finalization proof to marshal
                let round = finalized.proof.round();
                self.marshal_mailbox
                    .verified(round, finalized.block.clone())
                    .await;
                self.marshal_mailbox
                    .report(Activity::Finalization(finalized.proof.clone()))
                    .await;
                debug!(
                    height = height.get(),
                    view = view.get(),
                    "received finalization"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{MockSource, TestFixture};
    use alto_client::consensus::Message;
    use commonware_macros::test_traced;
    use commonware_runtime::{deterministic::Runner, Metrics, Runner as _};
    use commonware_utils::NZUsize;

    /// Verifies that a finalization with a valid threshold signature is
    /// accepted by handle_message without error.
    #[test_traced]
    fn accepts_valid_finalization() {
        let fixture = TestFixture::new();
        let finalized = fixture.create_finalized(1, 1);
        let verifier = fixture.verifier_scheme();

        Runner::default().start(|context| async move {
            // Engine is needed to provide the marshal mailbox
            let (_engine, mailbox, _) = crate::engine::Engine::new(
                context.with_label("engine"),
                verifier.clone(),
                16,
                NZUsize!(256),
                Sequential,
                None,
            )
            .await;

            let source = MockSource::new();
            let mut feeder = Feeder::new(context.with_label("feeder"), source, verifier, mailbox);

            let result = feeder
                .handle_message(Message::Finalization(finalized))
                .await;
            assert!(result.is_ok());
        });
    }

    /// Verifies that a finalization with an invalid threshold signature
    /// causes handle_message to panic (assertion failure).
    #[test_traced]
    #[should_panic(expected = "invalid finalization signature for height 1")]
    fn rejects_invalid_finalization() {
        let fixture = TestFixture::new();
        let finalized = fixture.create_finalized(1, 1);
        // Use a scheme derived from a different polynomial to force
        // threshold signature verification to fail.
        let wrong_verifier = fixture.wrong_verifier_scheme();

        Runner::default().start(|context| async move {
            let (_engine, mailbox, _) = crate::engine::Engine::new(
                context.with_label("engine"),
                wrong_verifier.clone(),
                16,
                NZUsize!(256),
                Sequential,
                None,
            )
            .await;

            let source = MockSource::new();
            let mut feeder = Feeder::new(
                context.with_label("feeder"),
                source,
                wrong_verifier,
                mailbox,
            );

            // Should panic on the assert! inside handle_message
            feeder
                .handle_message(Message::Finalization(finalized))
                .await
                .unwrap();
        });
    }

    /// Verifies that a notarization with a valid threshold signature is
    /// accepted by handle_message without error.
    #[test_traced]
    fn accepts_valid_notarization() {
        let fixture = TestFixture::new();
        let notarized = fixture.create_notarized(1, 1);
        let verifier = fixture.verifier_scheme();

        Runner::default().start(|context| async move {
            let (_engine, mailbox, _) = crate::engine::Engine::new(
                context.with_label("engine"),
                verifier.clone(),
                16,
                NZUsize!(256),
                Sequential,
                None,
            )
            .await;

            let source = MockSource::new();
            let mut feeder = Feeder::new(context.with_label("feeder"), source, verifier, mailbox);

            let result = feeder
                .handle_message(Message::Notarization(notarized))
                .await;
            assert!(result.is_ok());
        });
    }

    /// Verifies that a notarization with an invalid threshold signature
    /// causes handle_message to panic (assertion failure).
    #[test_traced]
    #[should_panic(expected = "invalid notarization signature for height 1")]
    fn rejects_invalid_notarization() {
        let fixture = TestFixture::new();
        let notarized = fixture.create_notarized(1, 1);
        let wrong_verifier = fixture.wrong_verifier_scheme();

        Runner::default().start(|context| async move {
            let (_engine, mailbox, _) = crate::engine::Engine::new(
                context.with_label("engine"),
                wrong_verifier.clone(),
                16,
                NZUsize!(256),
                Sequential,
                None,
            )
            .await;

            let source = MockSource::new();
            let mut feeder = Feeder::new(
                context.with_label("feeder"),
                source,
                wrong_verifier,
                mailbox,
            );

            feeder
                .handle_message(Message::Notarization(notarized))
                .await
                .unwrap();
        });
    }
}
