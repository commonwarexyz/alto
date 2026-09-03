use crate::{
    application::Application,
    indexer::{self, Client},
};
use alto_types::{Activity, Block, Finalization, Scheme, EPOCH, EPOCH_LENGTH};
use commonware_broadcast::buffered;
use commonware_consensus::{
    marshal::{
        self,
        core::{Actor as MarshalActor, Mailbox as MarshalMailbox},
        resolver::handler,
        standard::{Deferred, Standard},
    },
    simplex::{
        self,
        elector::{self, RoundRobin},
        Engine as Consensus,
    },
    types::{Epoch, FixedEpocher, TermLength, ViewDelta},
    Reporters,
};
use commonware_cryptography::{
    certificate::ConstantProvider,
    ed25519::PublicKey,
    sha256::{Digest, Sha256},
    Digestible as _,
};
use commonware_p2p::{Blocker, Provider, Receiver, Sender};
use commonware_parallel::Strategy;
use commonware_resolver::TargetedResolver;
use commonware_runtime::{
    buffer::paged::{page_size, CacheRef},
    spawn_cell, BufferPooler, Clock, ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::{archive::prunable, queue, translator::FourCap};
use commonware_utils::{NZUsize, NZU64};
use futures::future::try_join_all;
use governor::clock::Clock as GClock;
use governor::Quota;
use rand::{CryptoRng, Rng};
use std::{
    num::{NonZero, NonZeroUsize},
    time::{Duration, Instant},
};
use tracing::{error, info, warn};

/// Reporter type for [simplex::Engine].
type Reporter<E, C, CS> =
    Reporters<Activity<CS>, Option<indexer::Pusher<E, C, CS>>, MarshalMailbox<CS, Standard<Block>>>;

/// To better support peers near tip during network instability, we multiply
/// the consensus activity timeout by this factor.
const SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER: u64 = 10;
const PRUNABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(4_096);
const QUEUE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(128);
const ARCHIVE_COMPRESSION: Option<u8> = Some(3);
const REPLAY_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024); // 8MB
const WRITE_BUFFER: NonZero<usize> = NZUsize!(1024 * 1024); // 1MB
const PAGE_CACHE_PHYSICAL_PAGE_SIZE: u32 = 4_096;
const PAGE_CACHE_PAGE_SIZE: NonZero<u16> = page_size(PAGE_CACHE_PHYSICAL_PAGE_SIZE);
const PAGE_CACHE_CAPACITY: NonZero<usize> = NZUsize!(8_192); // 32MB
const MAX_REPAIR: NonZero<usize> = NZUsize!(20);
const MAX_PENDING_ACKS: NonZero<usize> = NZUsize!(16);
const STABLE_LEADER_STALL_TIMEOUT: Duration = Duration::from_secs(12);

/// Round-robin leader election used with native standard certificates.
pub type StableElector = RoundRobin<Sha256>;

/// Builds stable leader election with a bounded optimistic view window.
pub fn stable_elector(term_length: NonZero<u32>, optimistic_views: u64) -> StableElector {
    RoundRobin::default().with_term(
        TermLength::new(term_length),
        STABLE_LEADER_STALL_TIMEOUT,
        ViewDelta::new(optimistic_views),
    )
}

/// Configuration for the [Engine].
pub struct Config<
    B: Blocker<PublicKey = PublicKey>,
    P: Provider<PublicKey = PublicKey>,
    C: Client<CS>,
    S: Strategy,
    CS: Scheme,
    L: elector::Config<CS>,
> {
    pub blocker: B,
    pub provider: P,
    pub partition_prefix: String,
    pub me: PublicKey,
    pub scheme: CS,
    pub elector: L,
    pub mailbox_size: usize,
    pub deque_size: usize,
    pub block_size: u32,
    pub proposal_delay_ms: NonZero<u64>,

    pub leader_timeout: Duration,
    pub certification_timeout: Duration,
    pub nullify_retry: Duration,
    pub fetch_timeout: Duration,
    pub activity_timeout: ViewDelta,
    pub skip_timeout: Duration,
    pub max_fetch_count: usize,
    pub max_fetch_size: usize,
    pub fetch_rate_per_peer: Quota,
    pub backfiller_max_active: NonZeroUsize,
    pub backfiller_retry: Duration,

    pub strategy: S,

    pub indexer: Option<C>,
}

type Marshaled<E, CS> = Deferred<E, CS, Application<CS>, Block, FixedEpocher>;

/// The engine that drives the [Application].
#[allow(clippy::type_complexity)]
pub struct Engine<E, B, P, S, C, CS, L>
where
    E: BufferPooler + Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
    B: Blocker<PublicKey = PublicKey>,
    P: Provider<PublicKey = PublicKey>,
    S: Strategy,
    C: Client<CS>,
    CS: Scheme,
    L: elector::Config<CS>,
{
    context: ContextCell<E>,

    buffer: buffered::Engine<E, PublicKey, Block, P>,
    buffer_mailbox: buffered::Mailbox<PublicKey, Block>,
    marshal: MarshalActor<
        E,
        Standard<Block>,
        ConstantProvider<CS, Epoch>,
        prunable::Archive<FourCap, E, Digest, Finalization<CS>>,
        prunable::Archive<FourCap, E, Digest, Block>,
        FixedEpocher,
        S,
    >,
    marshaled: Marshaled<E, CS>,

    consensus:
        Consensus<E, CS, L, B, Digest, Marshaled<E, CS>, Marshaled<E, CS>, Reporter<E, C, CS>, S>,

    consumer: Option<indexer::Consumer<E, C, CS>>,
}

impl<E, B, P, S, C, CS, L> Engine<E, B, P, S, C, CS, L>
where
    E: BufferPooler + Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
    B: Blocker<PublicKey = PublicKey>,
    P: Provider<PublicKey = PublicKey>,
    S: Strategy,
    C: Client<CS>,
    CS: Scheme,
    L: elector::Config<CS>,
{
    /// Create a new [Engine].
    pub async fn new(context: E, cfg: Config<B, P, C, S, CS, L>) -> Self {
        let mailbox_size =
            NonZeroUsize::new(cfg.mailbox_size).expect("mailbox size must be non-zero");
        let proposal_delay_ms = cfg.proposal_delay_ms;

        // Create the buffer
        let (buffer, buffer_mailbox) = buffered::Engine::new(
            context.child("buffer"),
            buffered::Config {
                public_key: cfg.me,
                mailbox_size,
                deque_size: cfg.deque_size,
                priority: true,
                codec_config: (),
                peer_provider: cfg.provider,
            },
        );

        // Create the page cache
        let page_cache = CacheRef::from_pooler(&context, PAGE_CACHE_PAGE_SIZE, PAGE_CACHE_CAPACITY);

        // Validators retain all finalized history. These archives use the prunable write path,
        // but Alto never sends Marshal a request to prune them.
        let start = Instant::now();
        let finalizations_by_height = prunable::Archive::init(
            context.child("finalizations_by_height"),
            prunable::Config {
                translator: FourCap,
                metadata_partition: format!(
                    "{}-finalizations-by-height-metadata",
                    cfg.partition_prefix
                ),
                key_partition: format!("{}-finalizations-by-height-key", cfg.partition_prefix),
                key_page_cache: page_cache.clone(),
                value_partition: format!("{}-finalizations-by-height-value", cfg.partition_prefix),
                compression: ARCHIVE_COMPRESSION,
                codec_config: CS::certificate_codec_config_unbounded(),
                items_per_section: PRUNABLE_ITEMS_PER_SECTION,
                key_write_buffer: WRITE_BUFFER,
                value_write_buffer: WRITE_BUFFER,
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalizations by height archive");
        info!(elapsed = ?start.elapsed(), "restored finalizations by height archive");

        // Initialize finalized blocks
        let start = Instant::now();
        let finalized_blocks = prunable::Archive::init(
            context.child("finalized_blocks"),
            prunable::Config {
                translator: FourCap,
                metadata_partition: format!("{}-finalized-blocks-metadata", cfg.partition_prefix),
                key_partition: format!("{}-finalized-blocks-key", cfg.partition_prefix),
                key_page_cache: page_cache.clone(),
                value_partition: format!("{}-finalized-blocks-value", cfg.partition_prefix),
                compression: ARCHIVE_COMPRESSION,
                codec_config: (),
                items_per_section: PRUNABLE_ITEMS_PER_SECTION,
                key_write_buffer: WRITE_BUFFER,
                value_write_buffer: WRITE_BUFFER,
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalized blocks archive");
        info!(elapsed = ?start.elapsed(), "restored finalized blocks archive");

        // Create marshal
        let scheme = cfg.scheme;
        let provider = ConstantProvider::new(scheme.clone());
        let epocher = FixedEpocher::new(EPOCH_LENGTH);
        let genesis = Application::<CS>::genesis();
        let genesis_digest = genesis.digest();
        let (marshal, marshal_mailbox, _) = MarshalActor::init(
            context.child("marshal"),
            finalizations_by_height,
            finalized_blocks,
            marshal::Config {
                provider,
                epocher: epocher.clone(),
                partition_prefix: cfg.partition_prefix.clone(),
                mailbox_size,
                view_retention: ViewDelta::new(
                    cfg.activity_timeout
                        .get()
                        .saturating_mul(SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER),
                ),
                start: marshal::Start::Genesis(genesis),
                prunable_items_per_section: PRUNABLE_ITEMS_PER_SECTION,
                replay_buffer: REPLAY_BUFFER,
                key_write_buffer: WRITE_BUFFER,
                value_write_buffer: WRITE_BUFFER,
                block_codec_config: (),
                max_repair: MAX_REPAIR,
                max_pending_acks: MAX_PENDING_ACKS,
                page_cache: page_cache.clone(),
                strategy: cfg.strategy.clone(),
            },
        )
        .await;

        // Create the reporter and, when an indexer is configured, a backfill
        // queue of finalized digests so block uploads can resume after
        // restarts.
        let (app, pusher, consumer) = if let Some(indexer) = cfg.indexer {
            let queue = queue::shared::init(
                context.child("queue"),
                queue::Config {
                    partition: format!("{}-finalized-queue", cfg.partition_prefix),
                    items_per_section: QUEUE_ITEMS_PER_SECTION,
                    compression: None,
                    codec_config: (),
                    page_cache: page_cache.clone(),
                    write_buffer: WRITE_BUFFER,
                    replay_buffer: REPLAY_BUFFER,
                },
            )
            .await
            .expect("failed to initialize finalized queue");
            let indexer = indexer::Indexer::new(
                context.child("indexer"),
                indexer,
                marshal_mailbox.clone(),
                queue,
                mailbox_size,
                cfg.backfiller_max_active,
                cfg.backfiller_retry,
            )
            .await;
            let (producer, pusher, consumer) = indexer.split();
            let app = Application::new(proposal_delay_ms)
                .with_block_size(cfg.block_size)
                .with_backfiller(producer);
            (app, Some(pusher), Some(consumer))
        } else {
            (
                Application::new(proposal_delay_ms).with_block_size(cfg.block_size),
                None,
                None,
            )
        };

        // Create the application
        let marshaled = Marshaled::<E, CS>::new(
            context.child("marshaled"),
            app,
            marshal_mailbox.clone(),
            epocher,
        );

        // Create the reporter.
        let reporter: Reporter<E, C, CS> = (pusher, marshal_mailbox.clone()).into();

        // Create the consensus engine
        let consensus = Consensus::new(
            context.child("consensus"),
            simplex::Config {
                epoch: EPOCH,
                scheme,
                automaton: marshaled.clone(),
                relay: marshaled.clone(),
                reporter,
                track_historical_votes: false,
                partition: format!("{}-consensus", cfg.partition_prefix),
                mailbox_size,
                floor: simplex::Floor::Genesis(genesis_digest),
                leader_timeout: cfg.leader_timeout,
                certification_timeout: cfg.certification_timeout,
                timeout_retry: cfg.nullify_retry,
                fetch_timeout: cfg.fetch_timeout,
                view_retention: cfg.activity_timeout,
                skip: simplex::SkipPolicy::Enabled {
                    timeout: cfg.skip_timeout,
                    budget: simplex::SkipBudget::Participants,
                },
                forward: simplex::ForwardPolicy::Disabled,
                replay_buffer: REPLAY_BUFFER,
                write_buffer: WRITE_BUFFER,
                blocker: cfg.blocker,
                page_cache,
                elector: cfg.elector,
                strategy: cfg.strategy,
            },
        );

        // Return the engine
        Self {
            context: ContextCell::new(context),

            buffer,
            buffer_mailbox,
            marshal,
            marshaled,
            consensus,

            consumer,
        }
    }

    /// Start the [simplex::Engine].
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        mut self,
        pending: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        recovered: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        resolver: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        broadcast: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        marshal: (
            handler::Receiver<Digest>,
            impl TargetedResolver<
                Key = handler::Key<Digest>,
                Subscriber = handler::Annotation,
                PublicKey = PublicKey,
            >,
        ),
    ) -> Handle<()> {
        spawn_cell!(
            self.context,
            self.run(pending, recovered, resolver, broadcast, marshal)
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn run(
        self,
        pending: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        recovered: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        resolver: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        broadcast: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        marshal: (
            handler::Receiver<Digest>,
            impl TargetedResolver<
                Key = handler::Key<Digest>,
                Subscriber = handler::Annotation,
                PublicKey = PublicKey,
            >,
        ),
    ) {
        // Start the buffer
        let buffer_handle = self.buffer.start(broadcast);

        // Start marshal
        let marshal_handle = self
            .marshal
            .start(self.marshaled, self.buffer_mailbox, marshal);

        // Start draining queued block uploads before consensus so recovered work
        // resumes immediately on startup.
        let consumer_handle = self.consumer.map(indexer::Consumer::start);

        // Start consensus
        //
        // We start the application prior to consensus to ensure we can handle enqueued events from consensus (otherwise
        // restart could block).
        let consensus_handle = self.consensus.start(pending, recovered, resolver);

        // Wait for any actor to finish
        let mut handles: Vec<Handle<()>> = vec![buffer_handle, marshal_handle, consensus_handle];
        if let Some(h) = consumer_handle {
            handles.push(h);
        }
        if let Err(e) = try_join_all(handles).await {
            error!(?e, "engine failed");
        } else {
            warn!("engine stopped");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alto_types::{StandardScheme, VrfScheme, NAMESPACE, ROTATING_ELECTOR};
    use commonware_consensus::{
        simplex::scheme::bls12381_threshold::{standard, vrf},
        types::{Round, View},
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig,
        certificate::{mocks::Fixture, Scheme as _},
    };
    use commonware_utils::NZU32;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn electors_are_specialized_for_their_certificate_schemes() {
        let Fixture {
            schemes: vrf_schemes,
            ..
        } = vrf::fixture::<MinSig, _>(&mut StdRng::seed_from_u64(0), NAMESPACE, 4);
        let rotating =
            elector::Config::<VrfScheme>::build(ROTATING_ELECTOR, vrf_schemes[0].participants());
        assert_eq!(
            elector::Elector::terms(&rotating),
            elector::Terms::rotating()
        );

        let term_length = NZU32!(9);
        let Fixture {
            schemes: standard_schemes,
            ..
        } = standard::fixture::<MinSig, _>(&mut StdRng::seed_from_u64(1), NAMESPACE, 4);
        let stable = elector::Config::<StandardScheme>::build(
            stable_elector(term_length, 37),
            standard_schemes[0].participants(),
        );
        let terms = elector::Elector::terms(&stable);
        assert_eq!(terms.length(), TermLength::new(term_length));
        assert_eq!(terms.stall_timeout(), Some(STABLE_LEADER_STALL_TIMEOUT));
        assert_eq!(terms.optimistic_views(), ViewDelta::new(37));

        let first = elector::Elector::elect(&stable, Round::new(EPOCH, View::new(1)), None);
        let last = elector::Elector::elect(
            &stable,
            Round::new(EPOCH, View::new(term_length.get() as u64)),
            None,
        );
        let next = elector::Elector::elect(
            &stable,
            Round::new(EPOCH, View::new(u64::from(term_length.get()) + 1)),
            None,
        );
        assert_eq!(first, last);
        assert_ne!(first, next);
    }
}
