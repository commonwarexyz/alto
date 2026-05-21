use crate::{
    application::Application,
    archive::{
        self, Blocks, Certificates, PRUNABLE_ITEMS_PER_SECTION, REPLAY_BUFFER, WRITE_BUFFER,
    },
    resolver::Resolver,
};
use alto_types::{Block, Context, Scheme, EPOCH, EPOCH_LENGTH};
use commonware_broadcast::buffered;
use commonware_consensus::{
    marshal::{
        self,
        core::{Actor as MarshalActor, Mailbox as MarshalMailbox},
        resolver::handler,
        standard::Standard,
    },
    types::{FixedEpocher, Height, ViewDelta},
};
use commonware_cryptography::{
    certificate::ConstantProvider,
    ed25519::{PrivateKey, PublicKey},
    sha256::{self, Digest, Sha256},
    Digest as _, Hasher, Signer,
};
use commonware_parallel::Strategy;
use commonware_runtime::{
    spawn_cell, BufferPooler, ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_utils::NZUsize;
use futures::future::try_join_all;
use governor::clock::Clock as GClock;
use rand::{CryptoRng, Rng};
use std::num::{NonZero, NonZeroUsize};
use tracing::{error, warn};

const VIEW_RETENTION_TIMEOUT: ViewDelta = ViewDelta::new(2560);
const MAX_PENDING_ACKS: NonZero<usize> = NZUsize!(1024);
const GENESIS: &[u8] = b"commonware is neat";

fn genesis() -> Block {
    let genesis_context = Context {
        round: commonware_consensus::types::Round::new(
            EPOCH,
            commonware_consensus::types::View::zero(),
        ),
        leader: PrivateKey::from_seed(0).public_key(),
        parent: (
            commonware_consensus::types::View::zero(),
            sha256::Digest::EMPTY,
        ),
    };
    Block::new(
        genesis_context,
        Sha256::hash(GENESIS),
        commonware_consensus::types::Height::zero(),
        0,
    )
}

/// The engine that drives the follower's [MarshalActor].
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
    marshal: MarshalActor<
        E,
        Standard<Block>,
        ConstantProvider<Scheme, commonware_consensus::types::Epoch>,
        Certificates<E>,
        Blocks<E>,
        FixedEpocher,
        T,
    >,
    pruning_depth: Option<u64>,
    marshal_mailbox: MarshalMailbox<Scheme, Standard<Block>>,
    mailbox_size: NonZeroUsize,
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
        mailbox_size: NonZeroUsize,
        max_repair: NonZero<usize>,
        strategy: T,
        pruning_depth: Option<u64>,
    ) -> (Self, MarshalMailbox<Scheme, Standard<Block>>, Height) {
        // Initialize the finalized certificate and block archives. Uses
        // prunable archives when pruning is enabled, immutable otherwise.
        let (finalizations_by_height, finalized_blocks, page_cache) =
            archive::init(&mut context, &scheme, pruning_depth).await;

        // Create marshal
        let provider = ConstantProvider::new(scheme);
        let epocher = FixedEpocher::new(EPOCH_LENGTH);
        let (marshal, mailbox, last_processed_height) = MarshalActor::init(
            context.child("marshal"),
            finalizations_by_height,
            finalized_blocks,
            marshal::Config {
                provider,
                epocher,
                start: marshal::Start::Genesis(genesis()),
                partition_prefix: "follower-marshal".to_string(),
                mailbox_size,
                view_retention_timeout: VIEW_RETENTION_TIMEOUT,
                prunable_items_per_section: PRUNABLE_ITEMS_PER_SECTION,
                replay_buffer: REPLAY_BUFFER,
                key_write_buffer: WRITE_BUFFER,
                value_write_buffer: WRITE_BUFFER,
                block_codec_config: (),
                max_repair,
                max_pending_acks: MAX_PENDING_ACKS,
                page_cache,
                strategy,
            },
        )
        .await;

        // Return the engine and marshal mailbox
        let engine = Self {
            context: ContextCell::new(context),
            marshal,
            pruning_depth,
            marshal_mailbox: mailbox.clone(),
            mailbox_size,
        };
        (engine, mailbox, last_processed_height)
    }

    /// Start the [Engine].
    pub fn start(mut self, marshal: (handler::Receiver<Digest>, Resolver)) -> Handle<()> {
        spawn_cell!(self.context, self.run(marshal))
    }

    async fn run(mut self, marshal: (handler::Receiver<Digest>, Resolver)) {
        // Start the application actor
        let (app, mailbox) = Application::new(
            self.context.take(),
            self.marshal_mailbox,
            self.mailbox_size,
            self.pruning_depth,
        );
        let app_handle = app.start();

        // Start marshal
        let marshal_handle = self.marshal.start(
            mailbox,
            None::<buffered::Mailbox<PublicKey, Block>>,
            marshal,
        );

        // Wait for any actor to finish
        if let Err(e) = try_join_all(vec![marshal_handle, app_handle]).await {
            error!(?e, "engine failed");
        } else {
            warn!("engine stopped");
        }
    }
}
