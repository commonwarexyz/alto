use crate::{
    application::Application,
    indexer::{self, Client},
    Leader,
};
use alto_types::{
    Activity, Block, CodedBlock, CodingScheme, Commitment, Finalization, MarshalCoding, PublicKey,
    Scheme, StoredCodedBlock, EPOCH, EPOCH_LENGTH, NAMESPACE,
};
use commonware_coding::{CodecConfig, Config as CodingConfig};
use commonware_consensus::{
    marshal::{
        self,
        coding::{
            shards, types::coding_config_for_participants, Marshaled as CodingMarshaled,
            MarshaledConfig,
        },
        core::{Actor as MarshalActor, Mailbox as MarshalMailbox},
        resolver::handler,
    },
    simplex::{
        self,
        elector::{self, Random, RandomElector, RoundRobin, RoundRobinElector},
        Engine as Consensus,
    },
    types::{Epoch, FixedEpocher, Participant, Round, TermLength, ViewDelta},
    Reporters,
};
use commonware_cryptography::{
    bls12381::primitives::{group, sharing::Sharing, variant::MinSig},
    certificate::{ConstantProvider, Verifier as CertificateVerifier},
    sha256::{Digest, Sha256},
    Committable as _,
};
use commonware_p2p::{Blocker, Provider, Receiver, Sender};
use commonware_parallel::Strategy;
use commonware_resolver::TargetedResolver;
use commonware_runtime::{
    buffer::paged::{page_size, CacheRef},
    spawn_cell, BufferPooler, Clock, ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::{archive::immutable, queue};
use commonware_utils::{ordered::Set, NZUsize, NZU64};
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
type Reporter<E, C> =
    Reporters<Activity, Option<indexer::Pusher<E, C>>, MarshalMailbox<Scheme, MarshalCoding>>;

/// To better support peers near tip during network instability, we multiply
/// the consensus activity timeout by this factor.
const SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER: u64 = 10;
const PRUNABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(4_096);
const QUEUE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(128);
const IMMUTABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(262_144);
const FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;
const FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 2u32.pow(16); // 3MB
const FREEZER_JOURNAL_TARGET_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const FREEZER_JOURNAL_COMPRESSION: Option<u8> = Some(3);
const REPLAY_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024); // 8MB
const WRITE_BUFFER: NonZero<usize> = NZUsize!(1024 * 1024); // 1MB
const PAGE_CACHE_PHYSICAL_PAGE_SIZE: u32 = 4_096;
const PAGE_CACHE_PAGE_SIZE: NonZero<u16> = page_size(PAGE_CACHE_PHYSICAL_PAGE_SIZE);
const PAGE_CACHE_CAPACITY: NonZero<usize> = NZUsize!(8_192); // 32MB
const MAX_REPAIR: NonZero<usize> = NZUsize!(20);
const MAX_PENDING_ACKS: NonZero<usize> = NZUsize!(16);
const STABLE_LEADER_STALL_TIMEOUT: Duration = Duration::from_secs(12);
const STABLE_LEADER_OPTIMISTIC_VIEWS: ViewDelta = ViewDelta::new(100);

fn maximum_shard_size(max_message_size: u32, coding_config: CodingConfig) -> usize {
    let minimum_shards = usize::from(coding_config.minimum_shards.get());
    let encoded_block_bound =
        usize::try_from(max_message_size).expect("maximum message size is unsupported");
    // The transport bound already covers the encoded CodedBlock, including its coding config.
    // Reed-Solomon adds a four-byte length prefix before splitting that payload into shards.
    let mut shard_size = encoded_block_bound
        .checked_add(std::mem::size_of::<u32>())
        .expect("maximum message size overflowed")
        .div_ceil(minimum_shards);
    if !shard_size.is_multiple_of(2) {
        shard_size += 1;
    }
    shard_size
}

/// Adapts the serialized leader policy to Simplex's statically typed elector configuration.
#[derive(Clone, Default)]
struct ElectorConfig(Leader);

impl elector::Config<Scheme> for ElectorConfig {
    type Elector = ConfiguredElector;

    fn build(self, participants: &Set<PublicKey>) -> Self::Elector {
        match self.0 {
            Leader::Rotating { .. } => {
                ConfiguredElector::Rotating(elector::Config::<Scheme>::build(Random, participants))
            }
            Leader::Stable { term_length, .. } => {
                ConfiguredElector::Stable(elector::Config::<Scheme>::build(
                    RoundRobin::<Sha256>::default().with_term(
                        TermLength::new(term_length),
                        STABLE_LEADER_STALL_TIMEOUT,
                        STABLE_LEADER_OPTIMISTIC_VIEWS,
                    ),
                    participants,
                ))
            }
        }
    }
}

/// Holds the initialized runtime choice behind the single elector type required by Simplex.
#[derive(Clone)]
enum ConfiguredElector {
    Rotating(RandomElector<Scheme>),
    Stable(RoundRobinElector<Scheme>),
}

impl elector::Elector<Scheme> for ConfiguredElector {
    fn terms(&self) -> elector::Terms {
        match self {
            Self::Rotating(elector) => elector::Elector::terms(elector),
            Self::Stable(elector) => elector::Elector::terms(elector),
        }
    }

    fn elect(
        &self,
        round: Round,
        certificate: Option<&<Scheme as CertificateVerifier>::Certificate>,
    ) -> Participant {
        match self {
            Self::Rotating(elector) => elector::Elector::elect(elector, round, certificate),
            Self::Stable(elector) => elector::Elector::elect(elector, round, certificate),
        }
    }
}

/// Configuration for the [Engine].
pub struct Config<
    B: Blocker<PublicKey = PublicKey>,
    P: Provider<PublicKey = PublicKey>,
    C: Client,
    S: Strategy,
> {
    pub blocker: B,
    pub provider: P,
    pub partition_prefix: String,
    pub blocks_freezer_table_initial_size: u32,
    pub finalized_freezer_table_initial_size: u32,
    pub polynomial: Sharing<MinSig>,
    pub share: group::Share,
    pub participants: Set<PublicKey>,
    pub mailbox_size: usize,
    pub deque_size: usize,
    pub max_message_size: u32,
    pub block_size: u32,
    pub leader: Leader,

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

type Marshaled<E, S> = CodingMarshaled<
    E,
    Application,
    Block,
    CodingScheme,
    Sha256,
    ConstantProvider<Scheme, Epoch>,
    S,
    FixedEpocher,
>;

/// The engine that drives the [Application].
#[allow(clippy::type_complexity)]
pub struct Engine<E, B, P, S, C>
where
    E: BufferPooler + Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
    B: Blocker<PublicKey = PublicKey> + Clone,
    P: Provider<PublicKey = PublicKey>,
    S: Strategy,
    C: Client,
{
    context: ContextCell<E>,

    shards: shards::Engine<
        E,
        ConstantProvider<Scheme, Epoch>,
        B,
        P,
        CodingScheme,
        Sha256,
        Block,
        PublicKey,
        S,
    >,
    shards_mailbox: shards::Mailbox<Block, CodingScheme, Sha256, PublicKey>,
    marshal: MarshalActor<
        E,
        MarshalCoding,
        ConstantProvider<Scheme, Epoch>,
        immutable::Archive<E, Digest, Finalization>,
        immutable::Archive<E, Digest, StoredCodedBlock>,
        FixedEpocher,
        S,
    >,
    marshaled: Marshaled<E, S>,

    consensus: Consensus<
        E,
        Scheme,
        ElectorConfig,
        B,
        Commitment,
        Marshaled<E, S>,
        Marshaled<E, S>,
        Reporter<E, C>,
        S,
    >,

    consumer: Option<indexer::Consumer<E, C>>,
}

impl<E, B, P, S, C> Engine<E, B, P, S, C>
where
    E: BufferPooler + Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
    B: Blocker<PublicKey = PublicKey> + Clone,
    P: Provider<PublicKey = PublicKey>,
    S: Strategy,
    C: Client,
{
    /// Create a new [Engine].
    pub async fn new(context: E, cfg: Config<B, P, C, S>) -> Self {
        let mailbox_size =
            NonZeroUsize::new(cfg.mailbox_size).expect("mailbox size must be non-zero");
        let peer_buffer_size =
            NonZeroUsize::new(cfg.deque_size).expect("deque size must be non-zero");
        let participants = u16::try_from(cfg.participants.len())
            .expect("validator count must fit in the coding configuration");
        let coding_config = coding_config_for_participants(participants);
        let shard_size = maximum_shard_size(cfg.max_message_size, coding_config);
        let proposal_delay_ms = cfg.leader.delay_ms();
        let elector = ElectorConfig(cfg.leader);

        // Create the page cache
        let page_cache = CacheRef::from_pooler(&context, PAGE_CACHE_PAGE_SIZE, PAGE_CACHE_CAPACITY);

        // Initialize finalizations by height
        let start = Instant::now();
        let finalizations_by_height = immutable::Archive::init(
            context.child("finalizations_by_height"),
            immutable::Config {
                metadata_partition: format!(
                    "{}-finalizations-by-height-metadata",
                    cfg.partition_prefix
                ),
                freezer_table_partition: format!(
                    "{}-finalizations-by-height-freezer-table",
                    cfg.partition_prefix
                ),
                freezer_table_initial_size: cfg.finalized_freezer_table_initial_size,
                freezer_table_resize_frequency: FREEZER_TABLE_RESIZE_FREQUENCY,
                freezer_table_resize_chunk_size: FREEZER_TABLE_RESIZE_CHUNK_SIZE,
                freezer_key_partition: format!(
                    "{}-finalizations-by-height-freezer-key-journal",
                    cfg.partition_prefix
                ),
                freezer_key_page_cache: page_cache.clone(),
                freezer_key_write_buffer: WRITE_BUFFER,
                freezer_value_partition: format!(
                    "{}-finalizations-by-height-freezer-value-journal",
                    cfg.partition_prefix
                ),
                freezer_value_write_buffer: WRITE_BUFFER,
                freezer_value_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_value_compression: FREEZER_JOURNAL_COMPRESSION,
                ordinal_partition: format!(
                    "{}-finalizations-by-height-ordinal",
                    cfg.partition_prefix
                ),
                ordinal_write_buffer: WRITE_BUFFER,
                items_per_section: IMMUTABLE_ITEMS_PER_SECTION,
                codec_config: Scheme::certificate_codec_config_unbounded(),
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalizations by height archive");
        info!(elapsed = ?start.elapsed(), "restored finalizations by height archive");

        // Initialize finalized blocks
        let start = Instant::now();
        let finalized_blocks = immutable::Archive::init(
            context.child("finalized_blocks"),
            immutable::Config {
                metadata_partition: format!("{}-finalized_blocks-metadata", cfg.partition_prefix),
                freezer_table_partition: format!(
                    "{}-finalized_blocks-freezer-table",
                    cfg.partition_prefix
                ),
                freezer_table_initial_size: cfg.blocks_freezer_table_initial_size,
                freezer_table_resize_frequency: FREEZER_TABLE_RESIZE_FREQUENCY,
                freezer_table_resize_chunk_size: FREEZER_TABLE_RESIZE_CHUNK_SIZE,
                freezer_key_partition: format!(
                    "{}-finalized-blocks-freezer-key-journal",
                    cfg.partition_prefix
                ),
                freezer_key_page_cache: page_cache.clone(),
                freezer_key_write_buffer: WRITE_BUFFER,
                freezer_value_partition: format!(
                    "{}-finalized-blocks-freezer-value-journal",
                    cfg.partition_prefix
                ),
                freezer_value_write_buffer: WRITE_BUFFER,
                freezer_value_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_value_compression: FREEZER_JOURNAL_COMPRESSION,
                ordinal_partition: format!("{}-finalized-blocks-ordinal", cfg.partition_prefix),
                ordinal_write_buffer: WRITE_BUFFER,
                items_per_section: IMMUTABLE_ITEMS_PER_SECTION,
                codec_config: (),
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalized blocks archive");
        info!(elapsed = ?start.elapsed(), "restored finalized blocks archive");

        // Create marshal
        let scheme = Scheme::signer(NAMESPACE, cfg.participants, cfg.polynomial, cfg.share)
            .expect("failed to create scheme");
        let provider = ConstantProvider::new(scheme.clone());
        let epocher = FixedEpocher::new(EPOCH_LENGTH);
        let genesis = CodedBlock::new(Application::genesis(), coding_config, &cfg.strategy);
        let genesis_commitment = genesis.commitment();

        // Create the erasure-coded shard broadcaster.
        let (shards, shards_mailbox) = shards::Engine::new(
            context.child("shards"),
            shards::Config {
                scheme_provider: provider.clone(),
                blocker: cfg.blocker.clone(),
                shard_codec_cfg: CodecConfig {
                    maximum_shard_size: shard_size,
                },
                block_codec_cfg: (),
                strategy: cfg.strategy.clone(),
                mailbox_size,
                peer_buffer_size,
                background_channel_capacity: mailbox_size,
                peer_provider: cfg.provider,
            },
        );

        let (marshal, marshal_mailbox, _) = MarshalActor::init(
            context.child("marshal"),
            finalizations_by_height,
            finalized_blocks,
            marshal::Config {
                provider: provider.clone(),
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
        let marshaled = CodingMarshaled::new(
            context.child("marshaled"),
            MarshaledConfig {
                application: app,
                marshal: marshal_mailbox.clone(),
                shards: shards_mailbox.clone(),
                scheme_provider: provider,
                strategy: cfg.strategy.clone(),
                epocher,
            },
        );

        // Create the reporter.
        let reporter: Reporter<E, C> = (pusher, marshal_mailbox.clone()).into();

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
                floor: simplex::Floor::Genesis(genesis_commitment),
                leader_timeout: cfg.leader_timeout,
                certification_timeout: cfg.certification_timeout,
                timeout_retry: cfg.nullify_retry,
                fetch_timeout: cfg.fetch_timeout,
                view_retention: cfg.activity_timeout,
                skip_timeout: cfg.skip_timeout,
                forwarding: simplex::ForwardingPolicy::Disabled,
                replay_buffer: REPLAY_BUFFER,
                write_buffer: WRITE_BUFFER,
                blocker: cfg.blocker,
                page_cache,
                elector,
                strategy: cfg.strategy,
            },
        );

        // Return the engine
        Self {
            context: ContextCell::new(context),

            shards,
            shards_mailbox,
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
        shards: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        marshal: (
            handler::Receiver<Commitment>,
            impl TargetedResolver<
                Key = handler::Key<Commitment>,
                Subscriber = handler::Annotation,
                PublicKey = PublicKey,
            >,
        ),
    ) -> Handle<()> {
        spawn_cell!(
            self.context,
            self.run(pending, recovered, resolver, shards, marshal)
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
        shards: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        marshal: (
            handler::Receiver<Commitment>,
            impl TargetedResolver<
                Key = handler::Key<Commitment>,
                Subscriber = handler::Annotation,
                PublicKey = PublicKey,
            >,
        ),
    ) {
        // Start shard dissemination.
        let shards_handle = self.shards.start(shards);

        // Start marshal
        let marshal_handle = self
            .marshal
            .start(self.marshaled, self.shards_mailbox, marshal);

        // Start draining queued block uploads before consensus so recovered work
        // resumes immediately on startup.
        let consumer_handle = self.consumer.map(indexer::Consumer::start);

        // Start consensus
        //
        // We start the application prior to consensus to ensure we can handle enqueued events from consensus (otherwise
        // restart could block).
        let consensus_handle = self.consensus.start(pending, recovered, resolver);

        // Wait for any actor to finish
        let mut handles: Vec<Handle<()>> = vec![shards_handle, marshal_handle, consensus_handle];
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
    use bytes::Bytes;
    use commonware_codec::{Decode, Encode};
    use commonware_consensus::{
        marshal::coding::types::Shard,
        simplex::{scheme::bls12381_threshold::vrf, types::Subject},
        types::View,
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig,
        certificate::{mocks::Fixture, Scheme as _},
        sha256::Digest as Sha256Digest,
    };
    use commonware_parallel::Sequential;
    use commonware_utils::{Faults, N3f1, NZU32, NZU64};
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn one_mib_shards_fit_128_validator_transport() {
        const PARTICIPANTS: u16 = 128;
        const MAX_MESSAGE_SIZE: u32 = 2 * 1024 * 1024 + 4;

        let coding_config = coding_config_for_participants(PARTICIPANTS);
        let maximum_shard_size = maximum_shard_size(MAX_MESSAGE_SIZE, coding_config);
        let genesis = Application::genesis();
        let block = Block::new(
            genesis.context,
            genesis.parent,
            genesis.height,
            genesis.timestamp,
            Bytes::from(vec![0xa5; 1024 * 1024]),
        );
        let coded = CodedBlock::new(block, coding_config, &Sequential);
        let shard_codec = CodecConfig { maximum_shard_size };

        assert_eq!(coded.shards(&Sequential).len(), usize::from(PARTICIPANTS));
        for index in 0..PARTICIPANTS {
            let encoded = coded.shard(index).expect("shard should exist").encode();
            assert!(encoded.len() <= MAX_MESSAGE_SIZE as usize);
            Shard::<CodingScheme, Sha256>::decode_cfg(encoded, &shard_codec)
                .expect("configured shard limit should admit a 1 MiB block");
        }
    }

    #[test]
    fn configured_elector_preserves_rotating_and_stable_terms() {
        let Fixture { schemes, .. } =
            vrf::fixture::<MinSig, _>(&mut StdRng::seed_from_u64(0), NAMESPACE, 4);
        let participants = schemes[0].participants();

        let rotating_config = Leader::rotating(NZU64!(7));
        assert_eq!(rotating_config.delay_ms(), NZU64!(7));
        assert_eq!(Leader::default(), Leader::stable(NZU64!(10), NZU32!(1_000)));

        let rotating =
            elector::Config::<Scheme>::build(ElectorConfig(rotating_config), participants);
        assert_eq!(
            elector::Elector::terms(&rotating),
            elector::Terms::rotating()
        );
        let random = elector::Config::<Scheme>::build(Random, participants);
        let certificate_round = Round::new(EPOCH, View::new(1));
        let attestations: Vec<_> = schemes
            .iter()
            .take(N3f1::quorum(schemes.len()) as usize)
            .map(|scheme| {
                scheme
                    .sign::<Sha256Digest>(Subject::Nullify {
                        round: certificate_round,
                    })
                    .unwrap()
            })
            .collect();
        let certificate = schemes[0].assemble(attestations, &Sequential).unwrap();
        let next_round = Round::new(EPOCH, View::new(2));
        assert_eq!(
            elector::Elector::elect(&rotating, next_round, Some(&certificate)),
            elector::Elector::elect(&random, next_round, Some(&certificate)),
        );

        let term_length = NZU32!(9);
        let stable = elector::Config::<Scheme>::build(
            ElectorConfig(Leader::stable(NZU64!(10), term_length)),
            participants,
        );
        let terms = elector::Elector::terms(&stable);
        assert_eq!(terms.length(), TermLength::new(term_length));
        assert_eq!(terms.stall_timeout(), Some(STABLE_LEADER_STALL_TIMEOUT));
        assert_eq!(terms.optimistic_views(), STABLE_LEADER_OPTIMISTIC_VIEWS);

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
