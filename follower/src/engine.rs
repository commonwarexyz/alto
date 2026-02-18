use crate::{
    application::Application,
    archive::{
        self, Blocks, Certificates, PRUNABLE_ITEMS_PER_SECTION, REPLAY_BUFFER, WRITE_BUFFER,
    },
    resolver::Resolver,
    NoopReceiver, NoopSender,
};
use alto_types::{Block, Scheme, EPOCH_LENGTH};
use commonware_broadcast::buffered;
use commonware_consensus::{
    marshal::{self, ingress::handler},
    types::{FixedEpocher, Height, ViewDelta},
};
use commonware_cryptography::{
    certificate::ConstantProvider,
    ed25519::{PrivateKey, PublicKey},
    Signer,
};
use commonware_math::algebra::Random;
use commonware_parallel::Strategy;
use commonware_runtime::{
    spawn_cell, BufferPooler, ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_utils::{channel::mpsc, NZUsize};
use futures::future::try_join_all;
use governor::clock::Clock as GClock;
use rand::{CryptoRng, Rng};
use std::num::NonZero;
use tracing::{error, warn};

const VIEW_RETENTION_TIMEOUT: ViewDelta = ViewDelta::new(2560);
const DEQUE_SIZE: usize = 10;

/// The engine that drives the follower's [marshal::Actor].
///
/// Unlike the validator's engine, this does not run consensus. Instead, it
/// relies on a [Feeder](crate::feeder::Feeder) to feed certificates from a
/// trusted source and an [Actor](crate::resolver::Actor) to backfill missing
/// blocks.
#[allow(clippy::type_complexity)]
pub struct Engine<E, T>
where
    E: BufferPooler
        + commonware_runtime::Clock
        + GClock
        + Rng
        + CryptoRng
        + Spawner
        + Storage
        + Metrics,
    T: Strategy,
{
    context: ContextCell<E>,
    buffer: buffered::Engine<E, PublicKey, Block>,
    buffer_mailbox: buffered::Mailbox<PublicKey, Block>,
    marshal: marshal::Actor<
        E,
        Block,
        ConstantProvider<Scheme, commonware_consensus::types::Epoch>,
        Certificates<E>,
        Blocks<E>,
        FixedEpocher,
        T,
    >,
    pruning_depth: Option<u64>,
    marshal_mailbox: marshal::Mailbox<Scheme, Block>,
}

impl<E, T> Engine<E, T>
where
    E: BufferPooler
        + commonware_runtime::Clock
        + GClock
        + Rng
        + CryptoRng
        + Spawner
        + Storage
        + Metrics,
    T: Strategy,
{
    /// Create a new [Engine].
    pub async fn new(
        mut context: E,
        scheme: Scheme,
        mailbox_size: usize,
        max_repair: NonZero<usize>,
        strategy: T,
        pruning_depth: Option<u64>,
    ) -> (Self, marshal::Mailbox<Scheme, Block>, Height) {
        // Create the buffer
        //
        // The follower does not participate in p2p broadcast, so we use a dummy
        // key and noop sender/receiver. The buffer is still required by marshal.
        let dummy_key = PrivateKey::random(&mut context).public_key();
        let (buffer, buffer_mailbox) = buffered::Engine::new(
            context.with_label("buffer"),
            buffered::Config {
                public_key: dummy_key,
                mailbox_size,
                deque_size: DEQUE_SIZE,
                priority: false,
                codec_config: (),
            },
        );

        // Initialize the finalized certificate and block archives. Uses
        // prunable archives when pruning is enabled, immutable otherwise.
        let (finalizations_by_height, finalized_blocks, page_cache) =
            archive::init(&mut context, &scheme, pruning_depth).await;

        // Create marshal
        let provider = ConstantProvider::new(scheme);
        let epocher = FixedEpocher::new(EPOCH_LENGTH);
        let (marshal, mailbox, last_processed_height) = marshal::Actor::init(
            context.with_label("marshal"),
            finalizations_by_height,
            finalized_blocks,
            marshal::Config {
                provider,
                epocher,
                partition_prefix: "follower-marshal".to_string(),
                mailbox_size,
                view_retention_timeout: VIEW_RETENTION_TIMEOUT,
                prunable_items_per_section: PRUNABLE_ITEMS_PER_SECTION,
                replay_buffer: REPLAY_BUFFER,
                key_write_buffer: WRITE_BUFFER,
                value_write_buffer: WRITE_BUFFER,
                block_codec_config: (),
                max_repair,
                max_pending_acks: NZUsize!(1024),
                page_cache,
                strategy,
            },
        )
        .await;

        // Return the engine and marshal mailbox
        let engine = Self {
            context: ContextCell::new(context),
            buffer,
            buffer_mailbox,
            marshal,
            pruning_depth,
            marshal_mailbox: mailbox.clone(),
        };
        (engine, mailbox, last_processed_height)
    }

    /// Start the [Engine].
    pub fn start(
        mut self,
        marshal: (mpsc::Receiver<handler::Message<Block>>, Resolver),
    ) -> Handle<()> {
        spawn_cell!(self.context, self.run(marshal).await)
    }

    async fn run(mut self, marshal: (mpsc::Receiver<handler::Message<Block>>, Resolver)) {
        // Start the buffer
        let buffer_handle = self.buffer.start((NoopSender, NoopReceiver));

        // Start the application actor
        let (app, app_reporter) = Application::new(
            self.context.take(),
            self.marshal_mailbox,
            self.pruning_depth,
        );
        let app_handle = app.start();

        // Start marshal
        let marshal_handle = self.marshal.start(app_reporter, self.buffer_mailbox, marshal);

        // Wait for any actor to finish
        if let Err(e) = try_join_all(vec![buffer_handle, marshal_handle, app_handle]).await {
            error!(?e, "engine failed");
        } else {
            warn!("engine stopped");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Actor;
    use crate::test_utils::TestFixture;
    use alto_types::Block;
    use bytes::Bytes;
    use commonware_codec::Encode;
    use commonware_consensus::{
        marshal::ingress::handler,
        types::{Height, Round, View},
    };
    use commonware_macros::test_traced;
    use commonware_parallel::Sequential;
    use commonware_runtime::{deterministic::Runner, Metrics, Runner as _};
    use commonware_utils::channel::{mpsc, oneshot};
    use commonware_utils::NZUsize;

    /// Verifies that marshal's Deliver handler rejects a finalization whose
    /// threshold signature does not match the configured scheme. This is
    /// the resolver path's signature verification (as opposed to the feeder
    /// path tested in feeder::tests).
    #[test_traced]
    fn marshal_rejects_invalid_finalization_from_resolver() {
        let fixture = TestFixture::new();
        let finalized = fixture.create_finalized(1, 1);
        let wrong_verifier = fixture.wrong_verifier_scheme();

        Runner::default().start(|context| async move {
            let (engine, _mailbox, _) = Engine::new(
                context.with_label("engine"),
                wrong_verifier.clone(),
                16,
                NZUsize!(256),
                Sequential,
                None,
            )
            .await;

            // Wire up the resolver and start the engine
            let (ingress_tx, ingress_rx) = mpsc::channel(16);
            let source = crate::test_utils::MockSource::new();
            let (_, resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx.clone(),
                16,
            );
            let _engine_handle = engine.start((ingress_rx, resolver));

            // Manually inject a finalization into marshal's ingress channel,
            // bypassing the resolver actor to control the payload directly.
            let key = handler::Request::<Block>::Finalized {
                height: Height::new(1),
            };
            let value = Bytes::from((finalized.proof, finalized.block).encode().to_vec());
            let (response_tx, response_rx) = oneshot::channel();
            ingress_tx
                .send(handler::Message::Deliver {
                    key,
                    value,
                    response: response_tx,
                })
                .await
                .expect("send failed");

            // Marshal should reject the delivery due to signature mismatch
            let accepted = response_rx.await.expect("response dropped");
            assert!(
                !accepted,
                "marshal should reject finalization with invalid signature"
            );
        });
    }

    /// Verifies that marshal's Deliver handler rejects a notarization whose
    /// threshold signature does not match the configured scheme.
    #[test_traced]
    fn marshal_rejects_invalid_notarization_from_resolver() {
        let fixture = TestFixture::new();
        let notarized = fixture.create_notarized(1, 1);
        let wrong_verifier = fixture.wrong_verifier_scheme();

        Runner::default().start(|context| async move {
            let (engine, _mailbox, _) = Engine::new(
                context.with_label("engine"),
                wrong_verifier.clone(),
                16,
                NZUsize!(256),
                Sequential,
                None,
            )
            .await;

            let (ingress_tx, ingress_rx) = mpsc::channel(16);
            let source = crate::test_utils::MockSource::new();
            let (_, resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx.clone(),
                16,
            );
            let _engine_handle = engine.start((ingress_rx, resolver));

            // Inject a notarization directly into marshal's ingress channel
            let round = Round::new(alto_types::EPOCH, View::new(1));
            let key = handler::Request::<Block>::Notarized { round };
            let value = Bytes::from((notarized.proof, notarized.block).encode().to_vec());
            let (response_tx, response_rx) = oneshot::channel();
            ingress_tx
                .send(handler::Message::Deliver {
                    key,
                    value,
                    response: response_tx,
                })
                .await
                .expect("send failed");

            let accepted = response_rx.await.expect("response dropped");
            assert!(
                !accepted,
                "marshal should reject notarization with invalid signature"
            );
        });
    }
}
