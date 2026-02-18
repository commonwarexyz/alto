use alto_chain::{
    application::Application,
    indexer::{self, Indexer},
};
use alto_types::{Activity, Block, Finalization, Scheme, EPOCH, EPOCH_LENGTH, NAMESPACE};
use commonware_broadcast::buffered;
use commonware_consensus::{
    application::marshaled::Marshaled as ConsensusMarshaled,
    marshal::{self, ingress::handler},
    simplex::{self, elector::Random, Engine as Consensus},
    types::{Epoch, FixedEpocher, ViewDelta},
    Reporters,
};
use commonware_cryptography::{
    bls12381::primitives::{group, sharing::Sharing, variant::MinSig},
    certificate::{ConstantProvider, Scheme as _},
    ed25519::PublicKey,
    sha256::Digest,
};
use commonware_p2p::{Blocker, Receiver, Sender};
use commonware_parallel::Strategy;
use commonware_resolver::Resolver;
use commonware_runtime::{
    buffer::paged::CacheRef, spawn_cell, BufferPooler, Clock, ContextCell, Handle, Metrics,
    Spawner, Storage, ThreadPooler,
};
use commonware_storage::archive::immutable;
use commonware_utils::channel::mpsc;
use commonware_utils::{ordered::Set, NZU16};
use commonware_utils::{NZUsize, NZU64};
use futures::future::try_join_all;
use governor::clock::Clock as GClock;
use governor::Quota;
use rand::{CryptoRng, Rng};
use std::{
    num::NonZero,
    time::{Duration, Instant},
};
use tracing::{error, info, warn};

/// Reporter type for [simplex::Engine].
type Reporter<E, I> =
    Reporters<Activity, marshal::Mailbox<Scheme, Block>, Option<indexer::Pusher<E, I>>>;

/// To better support peers near tip during network instability, we multiply
/// the consensus activity timeout by this factor.
const SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER: u64 = 10;
const PRUNABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(4_096);
const IMMUTABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(262_144);
const FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;
const FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 2u32.pow(16); // 3MB
const FREEZER_JOURNAL_TARGET_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const FREEZER_JOURNAL_COMPRESSION: Option<u8> = Some(3);
const REPLAY_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024); // 8MB
const WRITE_BUFFER: NonZero<usize> = NZUsize!(1024 * 1024); // 1MB
const PAGE_CACHE_PAGE_SIZE: NonZero<u16> = NZU16!(4_096); // 4KB
const PAGE_CACHE_CAPACITY: NonZero<usize> = NZUsize!(8_192); // 32MB
const MAX_REPAIR: NonZero<usize> = NZUsize!(20);
const MAX_PENDING_ACKS: NonZero<usize> = NZUsize!(16);

/// Configuration for the [Engine].
pub struct Config<B: Blocker<PublicKey = PublicKey>, I: Indexer, S: Strategy> {
    pub blocker: B,
    pub partition_prefix: String,
    pub blocks_freezer_table_initial_size: u32,
    pub finalized_freezer_table_initial_size: u32,
    pub me: PublicKey,
    pub polynomial: Sharing<MinSig>,
    pub share: group::Share,
    pub participants: Set<PublicKey>,
    pub mailbox_size: usize,
    pub deque_size: usize,

    pub leader_timeout: Duration,
    pub notarization_timeout: Duration,
    pub nullify_retry: Duration,
    pub fetch_timeout: Duration,
    pub activity_timeout: ViewDelta,
    pub skip_timeout: ViewDelta,
    pub max_fetch_count: usize,
    pub max_fetch_size: usize,
    pub fetch_concurrent: usize,
    pub fetch_rate_per_peer: Quota,

    pub strategy: S,

    pub indexer: Option<I>,
}

type Marshaled<E> = ConsensusMarshaled<E, Scheme, Application, Block, FixedEpocher>;

/// The engine that drives the [Application].
#[allow(clippy::type_complexity)]
pub struct Engine<E, B, S, I>
where
    E: BufferPooler + Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
    B: Blocker<PublicKey = PublicKey>,
    S: Strategy,
    I: Indexer,
{
    context: ContextCell<E>,

    buffer: buffered::Engine<E, PublicKey, Block>,
    buffer_mailbox: buffered::Mailbox<PublicKey, Block>,
    marshal: marshal::Actor<
        E,
        Block,
        ConstantProvider<Scheme, Epoch>,
        immutable::Archive<E, Digest, Finalization>,
        immutable::Archive<E, Digest, Block>,
        FixedEpocher,
        S,
    >,
    marshaled: Marshaled<E>,

    consensus:
        Consensus<E, Scheme, Random, B, Digest, Marshaled<E>, Marshaled<E>, Reporter<E, I>, S>,
}

impl<E, B, S, I> Engine<E, B, S, I>
where
    E: BufferPooler + Clock + GClock + Rng + CryptoRng + Spawner + ThreadPooler + Storage + Metrics,
    B: Blocker<PublicKey = PublicKey>,
    S: Strategy,
    I: Indexer,
{
    /// Create a new [Engine].
    pub async fn new(context: E, cfg: Config<B, I, S>) -> Self {
        // Create the buffer
        let (buffer, buffer_mailbox) = buffered::Engine::new(
            context.with_label("buffer"),
            buffered::Config {
                public_key: cfg.me,
                mailbox_size: cfg.mailbox_size,
                deque_size: cfg.deque_size,
                priority: true,
                codec_config: (),
            },
        );

        // Create the page cache
        let page_cache = CacheRef::from_pooler(&context, PAGE_CACHE_PAGE_SIZE, PAGE_CACHE_CAPACITY);

        // Initialize finalizations by height
        let start = Instant::now();
        let finalizations_by_height = immutable::Archive::init(
            context.with_label("finalizations_by_height"),
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
            context.with_label("finalized_blocks"),
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
        let (marshal, marshal_mailbox, _) = marshal::Actor::init(
            context.with_label("marshal"),
            finalizations_by_height,
            finalized_blocks,
            marshal::Config {
                provider,
                epocher: epocher.clone(),
                partition_prefix: cfg.partition_prefix.clone(),
                mailbox_size: cfg.mailbox_size,
                view_retention_timeout: ViewDelta::new(
                    cfg.activity_timeout
                        .get()
                        .saturating_mul(SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER),
                ),
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

        // Create the application
        let app = Application::new();
        let marshaled = Marshaled::new(
            context.with_label("marshaled"),
            app,
            marshal_mailbox.clone(),
            epocher,
        );

        // Create the reporter
        let reporter = (
            marshal_mailbox.clone(),
            cfg.indexer.map(|indexer| {
                indexer::Pusher::new(
                    context.with_label("indexer"),
                    indexer,
                    marshal_mailbox.clone(),
                )
            }),
        )
            .into();

        // Create the consensus engine
        let consensus = Consensus::new(
            context.with_label("consensus"),
            simplex::Config {
                epoch: EPOCH,
                scheme,
                automaton: marshaled.clone(),
                relay: marshaled.clone(),
                reporter,
                partition: format!("{}-consensus", cfg.partition_prefix),
                mailbox_size: cfg.mailbox_size,
                leader_timeout: cfg.leader_timeout,
                notarization_timeout: cfg.notarization_timeout,
                nullify_retry: cfg.nullify_retry,
                fetch_timeout: cfg.fetch_timeout,
                activity_timeout: cfg.activity_timeout,
                skip_timeout: cfg.skip_timeout,
                fetch_concurrent: cfg.fetch_concurrent,
                replay_buffer: REPLAY_BUFFER,
                write_buffer: WRITE_BUFFER,
                blocker: cfg.blocker,
                page_cache,
                elector: Random,
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
            mpsc::Receiver<handler::Message<Block>>,
            impl Resolver<Key = handler::Request<Block>, PublicKey = PublicKey>,
        ),
    ) -> Handle<()> {
        spawn_cell!(
            self.context,
            self.run(pending, recovered, resolver, broadcast, marshal)
                .await
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
            mpsc::Receiver<handler::Message<Block>>,
            impl Resolver<Key = handler::Request<Block>, PublicKey = PublicKey>,
        ),
    ) {
        // Start the buffer
        let buffer_handle = self.buffer.start(broadcast);

        // Start marshal
        let marshal_handle = self
            .marshal
            .start(self.marshaled, self.buffer_mailbox, marshal);

        // Start consensus
        //
        // We start the application prior to consensus to ensure we can handle enqueued events from consensus (otherwise
        // restart could block).
        let consensus_handle = self.consensus.start(pending, recovered, resolver);

        // Wait for any actor to finish
        if let Err(e) = try_join_all(vec![buffer_handle, marshal_handle, consensus_handle]).await {
            error!(?e, "engine failed");
        } else {
            warn!("engine stopped");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alto_types::NAMESPACE;
    use commonware_consensus::{
        marshal, simplex::scheme::bls12381_threshold::vrf as bls12381_threshold, types::ViewDelta,
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig, certificate::mocks::Fixture, ed25519::PublicKey,
        Signer,
    };
    use commonware_macros::{select, test_traced};
    use commonware_p2p::{
        simulated::{self, Link, Network, Oracle, Receiver, Sender},
        Manager,
    };
    use commonware_parallel::Sequential;
    use commonware_runtime::{
        deterministic::{self, Runner},
        Clock, Metrics, Runner as _, Spawner,
    };
    use commonware_utils::{ordered::Set, NZU32};
        use governor::Quota;
    use crate::test_utils::MockIndexer;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use std::{
        collections::{HashMap, HashSet},
        num::NonZeroU32,
        time::Duration,
    };
    use tracing::info;

    /// Limit the freezer table size to 1MB because the deterministic runtime stores
    /// everything in RAM.
    const FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(14); // 1MB

    /// (Effectively) unlimited quota for tests.
    const TEST_QUOTA: Quota = Quota::per_second(NZU32!(u32::MAX));

    /// Registers all validators using the oracle.
    async fn register_validators(
        oracle: &mut Oracle<PublicKey, deterministic::Context>,
        validators: &[PublicKey],
    ) -> HashMap<
        PublicKey,
        (
            (
                Sender<PublicKey, deterministic::Context>,
                Receiver<PublicKey>,
            ),
            (
                Sender<PublicKey, deterministic::Context>,
                Receiver<PublicKey>,
            ),
            (
                Sender<PublicKey, deterministic::Context>,
                Receiver<PublicKey>,
            ),
            (
                Sender<PublicKey, deterministic::Context>,
                Receiver<PublicKey>,
            ),
            (
                Sender<PublicKey, deterministic::Context>,
                Receiver<PublicKey>,
            ),
        ),
    > {
        oracle
            .manager()
            .track(0, Set::from_iter_dedup(validators.iter().cloned()))
            .await;
        let mut registrations = HashMap::new();
        for validator in validators.iter() {
            let oracle = oracle.control(validator.clone());
            let (pending_sender, pending_receiver) = oracle.register(0, TEST_QUOTA).await.unwrap();
            let (recovered_sender, recovered_receiver) =
                oracle.register(1, TEST_QUOTA).await.unwrap();
            let (resolver_sender, resolver_receiver) =
                oracle.register(2, TEST_QUOTA).await.unwrap();
            let (broadcast_sender, broadcast_receiver) =
                oracle.register(3, TEST_QUOTA).await.unwrap();
            let (backfill_sender, backfill_receiver) =
                oracle.register(4, TEST_QUOTA).await.unwrap();
            registrations.insert(
                validator.clone(),
                (
                    (pending_sender, pending_receiver),
                    (recovered_sender, recovered_receiver),
                    (resolver_sender, resolver_receiver),
                    (broadcast_sender, broadcast_receiver),
                    (backfill_sender, backfill_receiver),
                ),
            );
        }
        registrations
    }

    /// Links (or unlinks) validators using the oracle.
    ///
    /// The `action` parameter determines the action (e.g. link, unlink) to take.
    /// The `restrict_to` function can be used to restrict the linking to certain connections,
    /// otherwise all validators will be linked to all other validators.
    async fn link_validators(
        oracle: &mut Oracle<PublicKey, deterministic::Context>,
        validators: &[PublicKey],
        link: Link,
        restrict_to: Option<fn(usize, usize, usize) -> bool>,
    ) {
        for (i1, v1) in validators.iter().enumerate() {
            for (i2, v2) in validators.iter().enumerate() {
                // Ignore self
                if v2 == v1 {
                    continue;
                }

                // Restrict to certain connections
                if let Some(f) = restrict_to {
                    if !f(validators.len(), i1, i2) {
                        continue;
                    }
                }

                // Add link
                oracle
                    .add_link(v1.clone(), v2.clone(), link.clone())
                    .await
                    .unwrap();
            }
        }
    }

    fn all_online(n: u32, seed: u64, link: Link, required: u64) -> String {
        // Create context
        let cfg = deterministic::Config::default().with_seed(seed);
        let executor = Runner::from(cfg);
        executor.start(|mut context| async move {
            // Create simulated network
            let (network, mut oracle) = Network::new(
                context.with_label("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                    disconnect_on_block: true,
                    tracked_peer_sets: Some(1),
                },
            );

            // Start network
            network.start();

            // Register participants
            let Fixture {
                schemes,
                private_keys,
                participants,
                ..
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, NAMESPACE, n);
            let mut registrations = register_validators(&mut oracle, &participants).await;
            let participants_set = Set::from_iter_dedup(participants.clone());

            // Link all validators
            link_validators(&mut oracle, &participants, link, None).await;

            // Create instances
            let mut public_keys = HashSet::new();
            for (signer, scheme) in private_keys.into_iter().zip(schemes) {
                // Create signer context
                let public_key = signer.public_key();
                public_keys.insert(public_key.clone());

                // Configure engine
                let uid = format!("validator_{public_key}");
                let config: Config<_, MockIndexer, _> = Config {
                    blocker: oracle.control(public_key.clone()),
                    partition_prefix: uid.clone(),
                    blocks_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                    finalized_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                    me: signer.public_key(),
                    polynomial: scheme.polynomial().clone(),
                    share: scheme.share().cloned().unwrap(),
                    participants: participants_set.clone(),
                    mailbox_size: 1024,
                    deque_size: 10,
                    leader_timeout: Duration::from_secs(1),
                    notarization_timeout: Duration::from_secs(2),
                    nullify_retry: Duration::from_secs(10),
                    fetch_timeout: Duration::from_secs(1),
                    activity_timeout: ViewDelta::new(10),
                    skip_timeout: ViewDelta::new(5),
                    max_fetch_count: 10,
                    max_fetch_size: 1024 * 512,
                    fetch_concurrent: 10,
                    fetch_rate_per_peer: Quota::per_second(NonZeroU32::new(10).unwrap()),
                    indexer: None,
                    strategy: Sequential,
                };
                let validator_context = context.with_label(&uid);

                // Get networking
                let (pending, recovered, resolver, broadcast, backfill) =
                    registrations.remove(&public_key).unwrap();

                // Configure marshal resolver
                let marshal_resolver_cfg = marshal::resolver::p2p::Config {
                    public_key: public_key.clone(),
                    provider: oracle.manager(),
                    blocker: oracle.control(public_key.clone()),
                    mailbox_size: 1024,
                    initial: Duration::from_secs(1),
                    timeout: Duration::from_secs(2),
                    fetch_retry_timeout: Duration::from_millis(100),
                    priority_requests: false,
                    priority_responses: false,
                };
                let marshal_resolver = marshal::resolver::p2p::init(
                    &validator_context.with_label("backfill"),
                    marshal_resolver_cfg,
                    backfill,
                );

                // Start engine
                let engine = Engine::new(validator_context.with_label("engine"), config).await;
                engine.start(pending, recovered, resolver, broadcast, marshal_resolver);
            }

            // Poll metrics
            loop {
                let metrics = context.encode();

                // Iterate over all lines
                let mut success = false;
                for line in metrics.lines() {
                    // Ensure it is a metrics line
                    if !line.starts_with("validator_") {
                        continue;
                    }

                    // Split metric and value
                    let mut parts = line.split_whitespace();
                    let metric = parts.next().unwrap();
                    let value = parts.next().unwrap();

                    // If ends with peers_blocked, ensure it is zero
                    if metric.ends_with("_peers_blocked") {
                        let value = value.parse::<u64>().unwrap();
                        assert_eq!(value, 0);
                    }

                    // If ends with contiguous_height, ensure it is at least required_container
                    if metric.ends_with("_marshal_processed_height") {
                        let value = value.parse::<u64>().unwrap();
                        if value >= required {
                            success = true;
                            break;
                        }
                    }
                }
                if success {
                    break;
                }

                // Still waiting for all validators to complete
                context.sleep(Duration::from_secs(1)).await;
            }
            context.auditor().state()
        })
    }

    #[test_traced]
    fn test_good_links() {
        let link = Link {
            latency: Duration::from_millis(10),
            jitter: Duration::from_millis(1),
            success_rate: 1.0,
        };
        for seed in 0..5 {
            let state = all_online(5, seed, link.clone(), 25);
            assert_eq!(state, all_online(5, seed, link.clone(), 25));
        }
    }

    #[test_traced]
    fn test_bad_links() {
        let link = Link {
            latency: Duration::from_millis(200),
            jitter: Duration::from_millis(150),
            success_rate: 0.75,
        };
        for seed in 0..5 {
            let state = all_online(5, seed, link.clone(), 25);
            assert_eq!(state, all_online(5, seed, link.clone(), 25));
        }
    }

    #[test_traced]
    fn test_1k() {
        let link = Link {
            latency: Duration::from_millis(80),
            jitter: Duration::from_millis(10),
            success_rate: 0.98,
        };
        all_online(10, 0, link.clone(), 1000);
    }

    #[test_traced]
    fn test_backfill() {
        // Create context
        let n = 5;
        let initial_container_required = 10;
        let final_container_required = 20;
        let executor = Runner::timed(Duration::from_secs(30));
        executor.start(|mut context| async move {
            // Create simulated network
            let (network, mut oracle) = Network::new(
                context.with_label("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                    disconnect_on_block: true,
                    tracked_peer_sets: Some(1),
                },
            );

            // Start network
            network.start();

            // Register participants
            // Register participants
            let Fixture {
                schemes,
                private_keys,
                participants,
                ..
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, NAMESPACE, n);
            let mut registrations = register_validators(&mut oracle, &participants).await;
            let participants_set = Set::from_iter_dedup(participants.clone());

            // Link all validators (except 0)
            let link = Link {
                latency: Duration::from_millis(10),
                jitter: Duration::from_millis(1),
                success_rate: 1.0,
            };
            link_validators(
                &mut oracle,
                &participants,
                link.clone(),
                Some(|_, i, j| ![i, j].contains(&0usize)),
            )
            .await;

            // Create instances
            for (idx, (signer, scheme)) in private_keys.iter().zip(schemes.iter()).enumerate() {
                // Skip first
                if idx == 0 {
                    continue;
                }

                // Configure engine
                let public_key = signer.public_key();
                let uid = format!("validator_{public_key}");
                let config: Config<_, MockIndexer, _> = Config {
                    blocker: oracle.control(public_key.clone()),
                    partition_prefix: uid.clone(),
                    blocks_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                    finalized_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                    me: signer.public_key(),
                    polynomial: scheme.polynomial().clone(),
                    share: scheme.share().cloned().unwrap(),
                    participants: participants_set.clone(),
                    mailbox_size: 1024,
                    deque_size: 10,
                    leader_timeout: Duration::from_secs(1),
                    notarization_timeout: Duration::from_secs(2),
                    nullify_retry: Duration::from_secs(10),
                    fetch_timeout: Duration::from_secs(1),
                    activity_timeout: ViewDelta::new(10),
                    skip_timeout: ViewDelta::new(5),
                    max_fetch_count: 10,
                    max_fetch_size: 1024 * 512,
                    fetch_concurrent: 10,
                    fetch_rate_per_peer: Quota::per_second(NonZeroU32::new(10).unwrap()),
                    indexer: None,
                    strategy: Sequential,
                };
                let validator_context = context.with_label(&uid);

                // Get networking
                let (pending, recovered, resolver, broadcast, backfill) =
                    registrations.remove(&public_key).unwrap();

                // Configure marshal resolver
                let marshal_resolver_cfg = marshal::resolver::p2p::Config {
                    public_key: public_key.clone(),
                    provider: oracle.manager(),
                    blocker: oracle.control(public_key.clone()),
                    mailbox_size: 1024,
                    initial: Duration::from_secs(1),
                    timeout: Duration::from_secs(2),
                    fetch_retry_timeout: Duration::from_millis(100),
                    priority_requests: false,
                    priority_responses: false,
                };
                let marshal_resolver = marshal::resolver::p2p::init(
                    &validator_context.with_label("backfill"),
                    marshal_resolver_cfg,
                    backfill,
                );

                // Start engine
                let engine = Engine::new(validator_context.with_label("engine"), config).await;
                engine.start(pending, recovered, resolver, broadcast, marshal_resolver);
            }

            // Poll metrics
            loop {
                let metrics = context.encode();

                // Iterate over all lines
                let mut success = false;
                for line in metrics.lines() {
                    // Ensure it is a metrics line
                    if !line.starts_with("validator_") {
                        continue;
                    }

                    // Split metric and value
                    let mut parts = line.split_whitespace();
                    let metric = parts.next().unwrap();
                    let value = parts.next().unwrap();

                    // If ends with peers_blocked, ensure it is zero
                    if metric.ends_with("_peers_blocked") {
                        let value = value.parse::<u64>().unwrap();
                        assert_eq!(value, 0);
                    }

                    // If ends with contiguous_height, ensure it is at least required_container
                    if metric.ends_with("_marshal_processed_height") {
                        let value = value.parse::<u64>().unwrap();
                        if value >= initial_container_required {
                            success = true;
                            break;
                        }
                    }
                }
                if success {
                    break;
                }

                // Still waiting for all validators to complete
                context.sleep(Duration::from_secs(1)).await;
            }

            // Link first peer
            link_validators(
                &mut oracle,
                &participants,
                link,
                Some(|_, i, j| [i, j].contains(&0usize) && ![i, j].contains(&1usize)),
            )
            .await;

            // Configure engine
            let signer = private_keys[0].clone();
            let share = schemes[0].share().cloned().unwrap();
            let public_key = signer.public_key();
            let uid = format!("validator_{public_key}");
            let config: Config<_, MockIndexer, _> = Config {
                blocker: oracle.control(public_key.clone()),
                partition_prefix: uid.clone(),
                blocks_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                finalized_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                me: signer.public_key(),
                polynomial: schemes[0].polynomial().clone(),
                share,
                participants: participants_set,
                mailbox_size: 1024,
                deque_size: 10,
                leader_timeout: Duration::from_secs(1),
                notarization_timeout: Duration::from_secs(2),
                nullify_retry: Duration::from_secs(10),
                fetch_timeout: Duration::from_secs(1),
                activity_timeout: ViewDelta::new(10),
                skip_timeout: ViewDelta::new(5),
                max_fetch_count: 10,
                max_fetch_size: 1024 * 512,
                fetch_concurrent: 10,
                fetch_rate_per_peer: Quota::per_second(NonZeroU32::new(10).unwrap()),
                indexer: None,
                strategy: Sequential,
            };
            let validator_context = context.with_label(&uid);

            // Get networking
            let (pending, recovered, resolver, broadcast, backfill) =
                registrations.remove(&public_key).unwrap();

            // Configure marshal resolver
            let marshal_resolver_cfg = marshal::resolver::p2p::Config {
                public_key: public_key.clone(),
                provider: oracle.manager(),
                blocker: oracle.control(public_key.clone()),
                mailbox_size: 1024,
                initial: Duration::from_secs(1),
                timeout: Duration::from_secs(2),
                fetch_retry_timeout: Duration::from_millis(100),
                priority_requests: false,
                priority_responses: false,
            };
            let marshal_resolver = marshal::resolver::p2p::init(
                &validator_context.with_label("backfill"),
                marshal_resolver_cfg,
                backfill,
            );

            // Start engine
            let engine = Engine::new(validator_context.with_label("engine"), config).await;
            engine.start(pending, recovered, resolver, broadcast, marshal_resolver);

            // Poll metrics
            loop {
                let metrics = context.encode();

                // Iterate over all lines
                let mut success = false;
                for line in metrics.lines() {
                    // Ensure it is a metrics line
                    if !line.starts_with("validator_") {
                        continue;
                    }

                    // Split metric and value
                    let mut parts = line.split_whitespace();
                    let metric = parts.next().unwrap();
                    let value = parts.next().unwrap();

                    // If ends with peers_blocked, ensure it is zero
                    if metric.ends_with("_peers_blocked") {
                        let value = value.parse::<u64>().unwrap();
                        assert_eq!(value, 0);
                    }

                    // If ends with contiguous_height, ensure it is at least required_container
                    if metric.ends_with("_marshal_processed_height") {
                        let value = value.parse::<u64>().unwrap();
                        if value >= final_container_required {
                            success = true;
                            break;
                        }
                    }
                }
                if success {
                    break;
                }

                // Still waiting for all validators to complete
                context.sleep(Duration::from_secs(1)).await;
            }
        });
    }

    #[test_traced]
    fn test_unclean_shutdown() {
        // Create context
        let n = 5;
        let required_container = 100;

        // Derive threshold
        let mut rng = StdRng::seed_from_u64(0);
        let fixture = bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, n);

        // Random restarts every x seconds
        let mut runs = 0;
        let mut prev_checkpoint = None;
        loop {
            // Setup run
            let Fixture {
                schemes,
                private_keys,
                participants,
                ..
            } = fixture.clone();
            let f = |mut context: deterministic::Context| async move {
                // Create simulated network
                let (network, mut oracle) = Network::new(
                    context.with_label("network"),
                    simulated::Config {
                        max_size: 1024 * 1024,
                        disconnect_on_block: true,
                        tracked_peer_sets: Some(1),
                    },
                );

                // Start network
                network.start();

                // Register participants
                let mut registrations = register_validators(&mut oracle, &participants).await;
                let participants_set = Set::from_iter_dedup(participants.clone());

                // Link all validators
                let link = Link {
                    latency: Duration::from_millis(10),
                    jitter: Duration::from_millis(1),
                    success_rate: 1.0,
                };
                link_validators(&mut oracle, &participants, link, None).await;

                // Create instances
                let mut public_keys = HashSet::new();
                for (signer, scheme) in private_keys.into_iter().zip(schemes) {
                    // Create signer context
                    let public_key = signer.public_key();
                    public_keys.insert(public_key.clone());

                    // Configure engine
                    let uid = format!("validator_{public_key}");
                    let config: Config<_, MockIndexer, _> = Config {
                        blocker: oracle.control(public_key.clone()),
                        partition_prefix: uid.clone(),
                        blocks_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                        finalized_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                        me: signer.public_key(),
                        polynomial: scheme.polynomial().clone(),
                        share: scheme.share().cloned().unwrap(),
                        participants: participants_set.clone(),
                        mailbox_size: 1024,
                        deque_size: 10,
                        leader_timeout: Duration::from_secs(1),
                        notarization_timeout: Duration::from_secs(2),
                        nullify_retry: Duration::from_secs(10),
                        fetch_timeout: Duration::from_secs(1),
                        activity_timeout: ViewDelta::new(10),
                        skip_timeout: ViewDelta::new(5),
                        max_fetch_count: 10,
                        max_fetch_size: 1024 * 512,
                        fetch_concurrent: 10,
                        fetch_rate_per_peer: Quota::per_second(NonZeroU32::new(10).unwrap()),
                        indexer: None,
                        strategy: Sequential,
                    };
                    let validator_context = context.with_label(&uid);

                    // Get networking
                    let (pending, recovered, resolver, broadcast, backfill) =
                        registrations.remove(&public_key).unwrap();

                    // Configure marshal resolver
                    let marshal_resolver_cfg = marshal::resolver::p2p::Config {
                        public_key: public_key.clone(),
                        provider: oracle.manager(),
                        blocker: oracle.control(public_key.clone()),
                        mailbox_size: 1024,
                        initial: Duration::from_secs(1),
                        timeout: Duration::from_secs(2),
                        fetch_retry_timeout: Duration::from_millis(100),
                        priority_requests: false,
                        priority_responses: false,
                    };
                    let marshal_resolver = marshal::resolver::p2p::init(
                        &validator_context.with_label("backfill"),
                        marshal_resolver_cfg,
                        backfill,
                    );

                    // Start engine
                    let engine = Engine::new(validator_context.with_label("engine"), config).await;
                    engine.start(pending, recovered, resolver, broadcast, marshal_resolver);
                }

                // Poll metrics
                let poller = context
                    .with_label("metrics")
                    .spawn(move |context| async move {
                        loop {
                            let metrics = context.encode();

                            // Iterate over all lines
                            let mut success = false;
                            for line in metrics.lines() {
                                // Ensure it is a metrics line
                                if !line.starts_with("validator_") {
                                    continue;
                                }

                                // Split metric and value
                                let mut parts = line.split_whitespace();
                                let metric = parts.next().unwrap();
                                let value = parts.next().unwrap();

                                // If ends with peers_blocked, ensure it is zero
                                if metric.ends_with("_peers_blocked") {
                                    let value = value.parse::<u64>().unwrap();
                                    assert_eq!(value, 0);
                                }

                                // If ends with contiguous_height, ensure it is at least required_container
                                if metric.ends_with("_marshal_processed_height") {
                                    let value = value.parse::<u64>().unwrap();
                                    if value >= required_container {
                                        success = true;
                                        break;
                                    }
                                }
                            }
                            if success {
                                break;
                            }

                            // Still waiting for all validators to complete
                            context.sleep(Duration::from_millis(10)).await;
                        }
                    });

                // Exit at random points until finished
                let wait =
                    context.gen_range(Duration::from_millis(10)..Duration::from_millis(1_000));

                // Wait for one to finish
                select! {
                    _ = poller => {
                        // Finished
                        true
                    },
                    _ = context.sleep(wait) => {
                        // Randomly exit
                        false
                    }
                }
            };

            // Handle run
            let (complete, checkpoint) = if let Some(prev_checkpoint) = prev_checkpoint {
                Runner::from(prev_checkpoint)
            } else {
                Runner::timed(Duration::from_secs(30))
            }
            .start_and_recover(f);

            // Check if we should exit
            if complete {
                break;
            }

            // Prepare for next run
            prev_checkpoint = Some(checkpoint);
            runs += 1;
        }
        assert!(runs > 1);
        info!(runs, "unclean shutdown recovery worked");
    }

    #[test_traced]
    fn test_indexer() {
        // Create context
        let n = 5;
        let required_container = 10;
        let executor = Runner::timed(Duration::from_secs(30));
        executor.start(|mut context| async move {
            // Create simulated network
            let (network, mut oracle) = Network::new(
                context.with_label("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                    disconnect_on_block: true,
                    tracked_peer_sets: Some(1),
                },
            );

            // Start network
            network.start();

            // Register participants
            let Fixture {
                schemes,
                private_keys,
                participants,
                ..
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, NAMESPACE, n);
            let mut registrations = register_validators(&mut oracle, &participants).await;
            let participants_set = Set::from_iter_dedup(participants.clone());

            // Link all validators
            let link = Link {
                latency: Duration::from_millis(10),
                jitter: Duration::from_millis(1),
                success_rate: 1.0,
            };
            link_validators(&mut oracle, &participants, link, None).await;

            // Derive threshold
            let identity = *schemes[0].polynomial().public();

            // Define mock indexer
            let indexer = MockIndexer::new("", identity);

            // Create instances
            let mut public_keys = HashSet::new();
            for (signer, scheme) in private_keys.into_iter().zip(schemes) {
                // Create signer context
                let public_key = signer.public_key();
                public_keys.insert(public_key.clone());

                // Configure engine
                let uid = format!("validator_{public_key}");
                let config: Config<_, MockIndexer, _> = Config {
                    blocker: oracle.control(public_key.clone()),
                    partition_prefix: uid.clone(),
                    blocks_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                    finalized_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                    me: signer.public_key(),
                    polynomial: scheme.polynomial().clone(),
                    share: scheme.share().cloned().unwrap(),
                    participants: participants_set.clone(),
                    mailbox_size: 1024,
                    deque_size: 10,
                    leader_timeout: Duration::from_secs(1),
                    notarization_timeout: Duration::from_secs(2),
                    nullify_retry: Duration::from_secs(10),
                    fetch_timeout: Duration::from_secs(1),
                    activity_timeout: ViewDelta::new(10),
                    skip_timeout: ViewDelta::new(5),
                    max_fetch_count: 10,
                    max_fetch_size: 1024 * 512,
                    fetch_concurrent: 10,
                    fetch_rate_per_peer: Quota::per_second(NonZeroU32::new(10).unwrap()),
                    indexer: Some(indexer.clone()),
                    strategy: Sequential,
                };
                let validator_context = context.with_label(&uid);

                // Get networking
                let (pending, recovered, resolver, broadcast, backfill) =
                    registrations.remove(&public_key).unwrap();

                // Configure marshal resolver
                let marshal_resolver_cfg = marshal::resolver::p2p::Config {
                    public_key: public_key.clone(),
                    provider: oracle.manager(),
                    blocker: oracle.control(public_key.clone()),
                    mailbox_size: 1024,
                    initial: Duration::from_secs(1),
                    timeout: Duration::from_secs(2),
                    fetch_retry_timeout: Duration::from_millis(100),
                    priority_requests: false,
                    priority_responses: false,
                };
                let marshal_resolver = marshal::resolver::p2p::init(
                    &validator_context.with_label("backfill"),
                    marshal_resolver_cfg,
                    backfill,
                );

                // Start engine
                let engine = Engine::new(validator_context.with_label("engine"), config).await;
                engine.start(pending, recovered, resolver, broadcast, marshal_resolver);
            }

            // Poll metrics
            loop {
                let metrics = context.encode();

                // Iterate over all lines
                let mut success = false;
                for line in metrics.lines() {
                    // Ensure it is a metrics line
                    if !line.starts_with("validator_") {
                        continue;
                    }

                    // Split metric and value
                    let mut parts = line.split_whitespace();
                    let metric = parts.next().unwrap();
                    let value = parts.next().unwrap();

                    // If ends with peers_blocked, ensure it is zero
                    if metric.ends_with("_peers_blocked") {
                        let value = value.parse::<u64>().unwrap();
                        assert_eq!(value, 0);
                    }

                    // If ends with contiguous_height, ensure it is at least required_container
                    if metric.ends_with("_marshal_processed_height") {
                        let value = value.parse::<u64>().unwrap();
                        if value >= required_container {
                            success = true;
                            break;
                        }
                    }
                }
                if success {
                    break;
                }

                // Still waiting for all validators to complete
                context.sleep(Duration::from_secs(1)).await;
            }

            // Check indexer uploads
            assert!(indexer.seed_seen.load(std::sync::atomic::Ordering::Relaxed));
            assert!(indexer
                .notarization_seen
                .load(std::sync::atomic::Ordering::Relaxed));
            assert!(indexer
                .finalization_seen
                .load(std::sync::atomic::Ordering::Relaxed));
        });
    }
}
