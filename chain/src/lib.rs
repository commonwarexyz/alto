use alto_types::CertificateMode;
use commonware_utils::{NZUsize, Probability, NZU32};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    time::Duration,
};

pub mod application;
pub mod engine;
pub mod indexer;
pub mod utils;

pub const DEFAULT_BACKFILLER_MAX_ACTIVE: NonZeroUsize = NZUsize!(16);
pub const DEFAULT_BACKFILLER_RETRY_MS: u64 = 1_000;
pub const DEFAULT_BLOCKING_THREADS: usize = 512;
pub const DEFAULT_STORAGE_BUFFER_POOL_MAX_PER_CLASS: NonZeroU32 = NZU32!(16_384);
pub const DEFAULT_NETWORK_BUFFER_POOL_MAX_PER_CLASS: NonZeroU32 = NZU32!(4_096);

/// How long validators wait for a leader's proposal before nullifying the view.
/// Proposal pacing stays below this deadline to leave time to propose.
pub const LEADER_TIMEOUT: Duration = Duration::from_secs(1);

fn default_backfiller_max_active() -> NonZeroUsize {
    DEFAULT_BACKFILLER_MAX_ACTIVE
}

fn default_backfiller_retry_ms() -> u64 {
    DEFAULT_BACKFILLER_RETRY_MS
}

fn default_blocking_threads() -> usize {
    DEFAULT_BLOCKING_THREADS
}

fn default_storage_buffer_pool_max_per_class() -> Option<NonZeroU32> {
    Some(DEFAULT_STORAGE_BUFFER_POOL_MAX_PER_CLASS)
}

fn default_network_buffer_pool_max_per_class() -> Option<NonZeroU32> {
    Some(DEFAULT_NETWORK_BUFFER_POOL_MAX_PER_CLASS)
}

fn deserialize_traces_sample_rate<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let rate = f64::deserialize(deserializer)?;
    if (0.0..=1.0).contains(&rate) {
        return Ok(rate);
    }
    Err(serde::de::Error::custom(
        "traces sample rate must be between 0 and 1",
    ))
}

fn deserialize_term_length<'de, D>(deserializer: D) -> Result<NonZeroU32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let term_length = NonZeroU32::deserialize(deserializer)?;
    if term_length.get() > 1 {
        return Ok(term_length);
    }
    Err(serde::de::Error::custom(
        "stable leader term length must be greater than 1",
    ))
}

/// Leader election policy for the consensus engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Leader {
    /// Select a new VRF-derived leader for every view.
    Rotating { delay_ms: NonZeroU64 },
    /// Keep one round-robin leader for a term and pace its proposals.
    Stable {
        delay_ms: NonZeroU64,
        #[serde(deserialize_with = "deserialize_term_length")]
        term_length: NonZeroU32,
        optimistic_views: u64,
    },
}

impl Leader {
    /// Creates a rotating leader configuration.
    pub const fn rotating(delay_ms: NonZeroU64) -> Self {
        Self::Rotating { delay_ms }
    }

    /// Creates a stable leader configuration.
    pub const fn stable(
        delay_ms: NonZeroU64,
        term_length: NonZeroU32,
        optimistic_views: u64,
    ) -> Self {
        assert!(
            term_length.get() > 1,
            "stable leader term length must be greater than 1"
        );
        Self::Stable {
            delay_ms,
            term_length,
            optimistic_views,
        }
    }

    /// Threshold certificate construction required by this leader policy.
    pub const fn certificate_mode(self) -> CertificateMode {
        match self {
            Self::Rotating { .. } => CertificateMode::Vrf,
            Self::Stable { .. } => CertificateMode::Standard,
        }
    }
}

/// Configuration for the [engine::Engine].
#[derive(Deserialize, Serialize)]
pub struct Config {
    pub private_key: String,
    pub share: String,
    pub polynomial: String,

    pub port: u16,
    pub metrics_port: u16,
    pub directory: String,
    pub worker_threads: usize,
    #[serde(default = "default_blocking_threads")]
    pub blocking_threads: usize,
    #[serde(default = "default_storage_buffer_pool_max_per_class")]
    pub storage_buffer_pool_max_per_class: Option<NonZeroU32>,
    #[serde(default = "default_network_buffer_pool_max_per_class")]
    pub network_buffer_pool_max_per_class: Option<NonZeroU32>,
    #[serde(default)]
    pub storage_buffer_pool_parallelism: Option<NonZeroUsize>,
    #[serde(default)]
    pub network_buffer_pool_parallelism: Option<NonZeroUsize>,
    pub log_level: String,

    /// Fraction of traces exported to the configured collector. Zero disables tracing.
    #[serde(default, deserialize_with = "deserialize_traces_sample_rate")]
    pub traces_sample_rate: f64,

    pub local: bool,
    pub allowed_peers: Vec<String>,
    pub bootstrappers: Vec<String>,

    pub mailbox_size: usize,
    pub deque_size: usize,
    #[serde(default)]
    pub block_size: u32,

    pub signature_threads: usize,

    /// Required leader policy, which determines the certificate format.
    pub leader: Leader,

    #[serde(default = "default_backfiller_max_active")]
    pub backfiller_max_active: NonZeroUsize,
    #[serde(default = "default_backfiller_retry_ms")]
    pub backfiller_retry_ms: u64,

    /// Optional base HTTP(S) URL for the indexer API.
    pub indexer: Option<String>,
}

impl Config {
    /// Fraction of traces exported to the configured collector as a [Probability].
    ///
    /// The rate is validated to `[0, 1]` when deserialized and rounded to the nearest
    /// part per billion. Positive rates remain at least one part per billion.
    pub fn traces_sample_probability(&self) -> Probability {
        traces_sample_probability(self.traces_sample_rate)
    }
}

fn traces_sample_probability(rate: f64) -> Probability {
    // Use an integer ratio because Probability::from_f64 requires exact multiples of 2^-64
    // and rejects common rates such as 0.0001. Keep positive rates from rounding to zero.
    const PARTS_PER_BILLION: u64 = 1_000_000_000;
    let mut numerator = (rate * PARTS_PER_BILLION as f64).round() as u64;
    if rate > 0.0 {
        numerator = numerator.max(1);
    }
    Probability::new(numerator.min(PARTS_PER_BILLION), PARTS_PER_BILLION)
        .expect("traces sample rate must be between 0 and 1")
}

/// A list of peers provided when a validator is run locally.
///
/// When run remotely, [`commonware_deployer::aws::Hosts`](https://docs.rs/commonware-deployer/latest/commonware_deployer/aws/struct.Hosts.html) is used instead.
#[derive(Deserialize, Serialize)]
pub struct Peers {
    pub addresses: HashMap<String, SocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alto_types::{Block, NAMESPACE};
    use commonware_consensus::{
        marshal,
        simplex::{
            elector,
            scheme::bls12381_threshold::{standard as bls12381_threshold, vrf},
        },
        types::ViewDelta,
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig, certificate::mocks::Fixture, ed25519::PublicKey,
        Digestible, Signer,
    };
    use commonware_macros::{select, test_traced};
    use commonware_p2p::{
        simulated::{self, Link, Network, Oracle, Receiver, Sender},
        Manager,
    };
    use commonware_parallel::Sequential;
    use commonware_runtime::{
        deterministic::{self, Runner},
        Clock, Metrics, Runner as _, Spawner, Supervisor as _,
    };
    use commonware_utils::{channel::oneshot, ordered::Set, probability, NZUsize, NZU32, NZU64};
    use engine::Engine;
    use governor::Quota;
    use indexer::mocks;
    use rand::{rngs::StdRng, RngExt, SeedableRng};
    use std::{collections::HashMap, num::NonZeroU32, time::Duration};
    use tracing::info;

    /// Limit the freezer table size to 1MB because the deterministic runtime stores
    /// everything in RAM.
    const FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(14); // 1MB

    /// (Effectively) unlimited quota for tests.
    const TEST_QUOTA: Quota = Quota::per_second(NZU32!(u32::MAX));

    /// Proposal delay of every simulated validator and the term length of stable leaders.
    const PROPOSAL_DELAY_MS: NonZeroU64 = NZU64!(10);
    const STABLE_LEADER_TERM_LENGTH: NonZeroU32 = NZU32!(1_000);

    /// Registers all validators using the oracle.
    async fn register_validators(
        oracle: &mut Oracle<PublicKey, deterministic::Context>,
        validators: &[PublicKey],
    ) -> HashMap<PublicKey, Registration> {
        oracle
            .manager()
            .track(0, Set::from_iter_dedup(validators.iter().cloned()));
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

    fn sum_validator_metric<T: std::str::FromStr + std::iter::Sum>(
        metrics: &str,
        suffix: &str,
        label_filter: Option<&str>,
    ) -> T
    where
        T::Err: std::fmt::Debug,
    {
        metrics
            .lines()
            .filter_map(|line| {
                let (name, labels, value) = validator_metric_sample(line)?;
                if !name.ends_with(suffix) {
                    return None;
                }
                if let Some(filter) = label_filter {
                    if !labels.is_some_and(|labels| {
                        labels
                            .to_ascii_lowercase()
                            .contains(&filter.to_ascii_lowercase())
                    }) {
                        return None;
                    }
                }
                Some(value.parse::<T>().unwrap())
            })
            .sum()
    }

    fn validator_metric_sample(line: &str) -> Option<(&str, Option<&str>, &str)> {
        let line = line.trim();
        if line.starts_with('#') {
            return None;
        }
        let mut parts = line.split_whitespace();
        let metric = parts.next()?;
        let value = parts.next()?;
        let (name, labels) = metric
            .split_once('{')
            .map_or((metric, None), |(name, labels)| {
                (name, Some(labels.trim_end_matches('}')))
            });
        if !name.starts_with("validator_") {
            return None;
        }
        Some((name, labels, value))
    }

    fn queue_outstanding(metrics: &str) -> i64 {
        sum_validator_metric::<i64>(metrics, "_queue_tip", None)
            - sum_validator_metric::<i64>(metrics, "_queue_floor", None)
    }

    fn queue_held(metrics: &str) -> i64 {
        sum_validator_metric::<i64>(metrics, "_queue_next", None)
            - sum_validator_metric::<i64>(metrics, "_queue_floor", None)
    }

    type Registration = (
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
    );

    #[derive(Clone)]
    struct ValidatorConfig {
        leader_timeout: Duration,
        certification_timeout: Duration,
        block_size: u32,
        backfiller_max_active: NonZeroUsize,
        backfiller_retry: Duration,
        indexer: Option<mocks::Client>,
    }

    impl Default for ValidatorConfig {
        fn default() -> Self {
            Self {
                leader_timeout: Duration::from_secs(1),
                certification_timeout: Duration::from_secs(2),
                block_size: 0,
                backfiller_max_active: DEFAULT_BACKFILLER_MAX_ACTIVE,
                backfiller_retry: Duration::from_millis(DEFAULT_BACKFILLER_RETRY_MS),
                indexer: None,
            }
        }
    }

    /// Stable-leader election used by every simulation that does not pick its own elector.
    fn stable_elector() -> engine::StableElector {
        engine::stable_elector(STABLE_LEADER_TERM_LENGTH, 48)
    }

    async fn start_validator(
        context: &deterministic::Context,
        oracle: &Oracle<PublicKey, deterministic::Context>,
        signer: &commonware_cryptography::ed25519::PrivateKey,
        scheme: &bls12381_threshold::Scheme<PublicKey, MinSig>,
        registration: Registration,
        indexer: Option<mocks::Client>,
    ) {
        start_validator_with(
            context,
            oracle,
            signer,
            scheme,
            stable_elector(),
            registration,
            ValidatorConfig {
                indexer,
                ..Default::default()
            },
        )
        .await;
    }

    /// Starts a validator with an explicit certificate scheme and leader election policy.
    async fn start_validator_with<CS: alto_types::Scheme, L: elector::Config<CS>>(
        context: &deterministic::Context,
        oracle: &Oracle<PublicKey, deterministic::Context>,
        signer: &commonware_cryptography::ed25519::PrivateKey,
        scheme: &CS,
        elector: L,
        registration: Registration,
        cfg: ValidatorConfig,
    ) {
        let timeout_retry = cfg.certification_timeout + Duration::from_millis(50);
        let skip_timeout = timeout_retry + Duration::from_millis(50);

        let public_key = signer.public_key();
        let uid = format!("validator_{public_key}");
        let config = engine::Config {
            blocker: oracle.control(public_key.clone()),
            provider: oracle.manager(),
            partition_prefix: uid.clone(),
            blocks_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
            finalized_freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
            me: signer.public_key(),
            scheme: scheme.clone(),
            elector,
            mailbox_size: 1024,
            deque_size: 10,
            block_size: cfg.block_size,
            proposal_delay_ms: PROPOSAL_DELAY_MS,
            leader_timeout: cfg.leader_timeout,
            certification_timeout: cfg.certification_timeout,
            nullify_retry: timeout_retry,
            fetch_timeout: Duration::from_secs(1),
            activity_timeout: ViewDelta::new(10),
            skip_timeout,
            backfiller_max_active: cfg.backfiller_max_active,
            backfiller_retry: cfg.backfiller_retry,
            indexer: cfg.indexer,
            strategy: Sequential,
        };
        let validator_context = context.child("validator").with_attribute("id", &uid);
        let (pending, recovered, resolver, broadcast, backfill) = registration;
        let marshal_resolver_cfg = marshal::resolver::p2p::Config {
            public_key: public_key.clone(),
            peer_provider: oracle.manager(),
            blocker: oracle.control(public_key.clone()),
            mailbox_size: NZUsize!(1024),
            timeout: Duration::from_secs(2),
            fetch_retry_timeout: Duration::from_millis(100),
            priority_requests: false,
            priority_responses: false,
        };
        let marshal_resolver = marshal::resolver::p2p::init(
            validator_context.child("backfill"),
            marshal_resolver_cfg,
            backfill,
        );
        let engine = Engine::new(validator_context.child("engine"), config).await;
        engine.start(pending, recovered, resolver, broadcast, marshal_resolver);
    }

    #[test]
    fn traces_sample_probability_preserves_configured_rate() {
        assert!(traces_sample_probability(0.0).is_zero());
        assert!(traces_sample_probability(1.0).is_one());

        // Rates that are not exact multiples of 2^-64 (rejected by `Probability::from_f64`) must
        // still convert.
        assert!(Probability::from_f64(0.0001).is_none());
        let rate = traces_sample_probability(0.0001).as_f64();
        assert!((rate - 0.0001).abs() < 1e-12, "{rate}");

        // Positive rates below the ratio's resolution still keep tracing enabled.
        assert!(!traces_sample_probability(1e-12).is_zero());
    }

    async fn poll_until_height(
        context: &deterministic::Context,
        oracle: &Oracle<PublicKey, deterministic::Context>,
        required: u64,
    ) {
        loop {
            let metrics = context.encode();
            let mut success = false;
            for line in metrics.lines() {
                let Some((metric, _, value)) = validator_metric_sample(line) else {
                    continue;
                };
                if metric.ends_with("_marshal_processed_height") {
                    let value = value.parse::<u64>().unwrap();
                    if value >= required {
                        success = true;
                        break;
                    }
                }
            }

            // No validator should ever block a peer (checked after the height scan so the
            // final iteration is covered too).
            let blocked = oracle.blocked().await.expect("network closed");
            assert!(blocked.is_empty(), "peers blocked: {blocked:?}");
            if success {
                break;
            }
            context.sleep(Duration::from_secs(1)).await;
        }
    }

    fn all_online(n: u32, seed: u64, link: Link, required: u64) -> String {
        let cfg = deterministic::Config::default().with_seed(seed);
        let executor = Runner::from(cfg);
        executor.start(|mut context| async move {
            let (network, mut oracle) = Network::new(
                context.child("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                    max_peers_per_set: NZUsize!(n as usize),
                    disconnect_on_block: true,
                    tracked_peer_sets: NZUsize!(1),
                },
            );
            network.start();

            let Fixture {
                schemes,
                private_keys,
                participants,
                ..
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, NAMESPACE, n);
            let mut registrations = register_validators(&mut oracle, &participants).await;

            link_validators(&mut oracle, &participants, link, None).await;

            for (signer, scheme) in private_keys.iter().zip(schemes.iter()) {
                let registration = registrations.remove(&signer.public_key()).unwrap();
                start_validator(&context, &oracle, signer, scheme, registration, None).await;
            }

            poll_until_height(&context, &oracle, required).await;
            context.auditor().state()
        })
    }

    #[test_traced]
    fn test_good_links() {
        let link = Link {
            latency: Duration::from_millis(10),
            jitter: Duration::from_millis(1),
            success_rate: probability!(1.0),
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
            success_rate: probability!(0.75),
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
            success_rate: probability!(0.98),
        };
        all_online(10, 0, link.clone(), 1000);
    }

    #[test_traced]
    fn test_backfill() {
        let n = 5;
        let initial_container_required = 10;
        let final_container_required = 20;
        let executor = Runner::timed(Duration::from_secs(30));
        executor.start(|mut context| async move {
            let (network, mut oracle) = Network::new(
                context.child("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                    max_peers_per_set: NZUsize!(n as usize),
                    disconnect_on_block: true,
                    tracked_peer_sets: NZUsize!(1),
                },
            );
            network.start();

            let Fixture {
                schemes,
                private_keys,
                participants,
                ..
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, NAMESPACE, n);
            let mut registrations = register_validators(&mut oracle, &participants).await;

            // Link all validators (except 0)
            let link = Link {
                latency: Duration::from_millis(10),
                jitter: Duration::from_millis(1),
                success_rate: probability!(1.0),
            };
            link_validators(
                &mut oracle,
                &participants,
                link.clone(),
                Some(|_, i, j| ![i, j].contains(&0usize)),
            )
            .await;

            for (idx, (signer, scheme)) in private_keys.iter().zip(schemes.iter()).enumerate() {
                if idx == 0 {
                    continue;
                }
                let registration = registrations.remove(&signer.public_key()).unwrap();
                start_validator(&context, &oracle, signer, scheme, registration, None).await;
            }

            poll_until_height(&context, &oracle, initial_container_required).await;

            // Link first peer
            link_validators(
                &mut oracle,
                &participants,
                link,
                Some(|_, i, j| [i, j].contains(&0usize) && ![i, j].contains(&1usize)),
            )
            .await;

            let registration = registrations.remove(&private_keys[0].public_key()).unwrap();
            start_validator(
                &context,
                &oracle,
                &private_keys[0],
                &schemes[0],
                registration,
                None,
            )
            .await;

            poll_until_height(&context, &oracle, final_container_required).await;
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
                    context.child("network"),
                    simulated::Config {
                        max_size: 1024 * 1024,
                        max_peers_per_set: NZUsize!(n as usize),
                        disconnect_on_block: true,
                        tracked_peer_sets: NZUsize!(1),
                    },
                );

                // Start network
                network.start();

                // Register participants
                let mut registrations = register_validators(&mut oracle, &participants).await;

                // Link all validators
                let link = Link {
                    latency: Duration::from_millis(10),
                    jitter: Duration::from_millis(1),
                    success_rate: probability!(1.0),
                };
                link_validators(&mut oracle, &participants, link, None).await;

                // This test restarts validators every 250..1_000ms of simulated time.
                // Keep recovery timeouts below that window so a recovered view can
                // either certify or timeout/nullify before the next forced shutdown.
                // A non-zero payload also exercises the block codec bound across restarts (the
                // stored genesis block has an empty payload and must still decode).
                let cfg = ValidatorConfig {
                    leader_timeout: Duration::from_millis(250),
                    certification_timeout: Duration::from_millis(500),
                    block_size: 64,
                    ..Default::default()
                };
                for (signer, scheme) in private_keys.iter().zip(schemes.iter()) {
                    let registration = registrations.remove(&signer.public_key()).unwrap();
                    start_validator_with(
                        &context,
                        &oracle,
                        signer,
                        scheme,
                        stable_elector(),
                        registration,
                        cfg.clone(),
                    )
                    .await;
                }

                let poller_oracle = oracle.clone();
                let poller = context.child("metrics").spawn(move |context| async move {
                    loop {
                        let metrics = context.encode();

                        // Iterate over all lines
                        let mut success = false;
                        for line in metrics.lines() {
                            let Some((metric, _, value)) = validator_metric_sample(line) else {
                                continue;
                            };

                            // If ends with contiguous_height, ensure it is at least required_container
                            if metric.ends_with("_marshal_processed_height") {
                                let value = value.parse::<u64>().unwrap();
                                if value >= required_container {
                                    success = true;
                                    break;
                                }
                            }
                        }

                        // No validator should ever block a peer (checked after the height scan
                        // so the final iteration is covered too).
                        let blocked = poller_oracle.blocked().await.expect("network closed");
                        assert!(blocked.is_empty(), "peers blocked: {blocked:?}");
                        if success {
                            break;
                        }

                        // Still waiting for all validators to complete
                        context.sleep(Duration::from_millis(10)).await;
                    }
                });

                // Exit at random points until finished
                let wait =
                    context.random_range(Duration::from_millis(250)..Duration::from_millis(1_000));

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

    /// Runs validators with an indexer and checks what they upload: certificates always, seeds
    /// only when the scheme produces them, and bare blocks only for genesis.
    fn indexer_simulation<CS: alto_types::Scheme, L: elector::Config<CS>>(
        fixture: impl FnOnce(&mut deterministic::Context, u32) -> Fixture<CS> + Send + 'static,
        elector: impl Fn() -> L + Send + 'static,
        expect_seeds: bool,
    ) {
        // Create context
        let n = 5;
        let required_container = 10;
        let executor = Runner::timed(Duration::from_secs(30));
        executor.start(|mut context| async move {
            // Create simulated network
            let (network, mut oracle) = Network::new(
                context.child("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                    max_peers_per_set: NZUsize!(n as usize),
                    disconnect_on_block: true,
                    tracked_peer_sets: NZUsize!(1),
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
            } = fixture(&mut context, n);
            let mut registrations = register_validators(&mut oracle, &participants).await;

            // Link all validators
            let link = Link {
                latency: Duration::from_millis(10),
                jitter: Duration::from_millis(1),
                success_rate: probability!(1.0),
            };
            link_validators(&mut oracle, &participants, link, None).await;

            // Define mock indexer
            let indexer = mocks::Client::new();

            for (signer, scheme) in private_keys.into_iter().zip(schemes) {
                let registration = registrations.remove(&signer.public_key()).unwrap();
                start_validator_with(
                    &context,
                    &oracle,
                    &signer,
                    &scheme,
                    elector(),
                    registration,
                    ValidatorConfig {
                        indexer: Some(indexer.clone()),
                        ..Default::default()
                    },
                )
                .await;
            }

            poll_until_height(&context, &oracle, required_container).await;

            // Check indexer uploads
            assert_eq!(
                indexer.seed_seen.load(std::sync::atomic::Ordering::Relaxed),
                expect_seeds
            );
            assert!(indexer
                .notarization_seen
                .load(std::sync::atomic::Ordering::Relaxed));
            assert!(indexer
                .finalization_seen
                .load(std::sync::atomic::Ordering::Relaxed));
            let genesis_digest = Block::genesis().digest();
            let started_digests = indexer.block_upload_started_digests.lock().clone();
            let expected_genesis_uploads = n as usize;
            assert_eq!(
                started_digests.len(),
                expected_genesis_uploads,
                "only genesis should be uploaded as a bare block when certified uploads succeed",
            );
            assert!(
                started_digests
                    .iter()
                    .all(|digest| *digest == genesis_digest),
                "non-genesis block uploads should stay idle when certified uploads succeed",
            );
        });
    }

    #[test_traced]
    fn test_indexer() {
        // Standard certificates carry no seed.
        indexer_simulation(
            |context, n| bls12381_threshold::fixture::<MinSig, _>(context, NAMESPACE, n),
            stable_elector,
            false,
        );
    }

    #[test_traced]
    fn test_indexer_rotating() {
        // VRF certificates carry seeds, which validators publish to the indexer.
        indexer_simulation(
            |context, n| vrf::fixture::<MinSig, _>(context, NAMESPACE, n),
            || alto_types::ROTATING_ELECTOR,
            true,
        );
    }

    #[test_traced]
    fn test_drainer_fallback() {
        let n = 5;
        let required_container = 10;
        let executor = Runner::timed(Duration::from_secs(30));
        executor.start(|mut context| async move {
            let (network, mut oracle) = Network::new(
                context.child("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                    max_peers_per_set: NZUsize!(n as usize),
                    disconnect_on_block: true,
                    tracked_peer_sets: NZUsize!(1),
                },
            );
            network.start();

            // Stand up a normal validator set; only the mock indexer behavior
            // differs from the happy-path tests below.
            let Fixture {
                schemes,
                private_keys,
                participants,
                ..
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, NAMESPACE, n);
            let mut registrations = register_validators(&mut oracle, &participants).await;

            let link = Link {
                latency: Duration::from_millis(10),
                jitter: Duration::from_millis(1),
                success_rate: probability!(1.0),
            };
            link_validators(&mut oracle, &participants, link, None).await;

            // Reject certificate uploads so the only way blocks can reach the
            // indexer is through the durable block consumer.

            let indexer = mocks::Client::new().with_fail_certs();

            for (signer, scheme) in private_keys.into_iter().zip(schemes) {
                let registration = registrations.remove(&signer.public_key()).unwrap();
                start_validator(
                    &context,
                    &oracle,
                    &signer,
                    &scheme,
                    registration,
                    Some(indexer.clone()),
                )
                .await;
            }

            poll_until_height(&context, &oracle, required_container).await;

            // The mock rejects certified uploads, so both cert paths should
            // remain unsuccessful throughout the run.
            assert!(!indexer
                .notarization_seen
                .load(std::sync::atomic::Ordering::Relaxed));
            assert!(!indexer
                .finalization_seen
                .load(std::sync::atomic::Ordering::Relaxed));
            // The durable consumer should compensate by uploading blocks.
            for _ in 0..10 {
                if indexer
                    .block_upload_completed
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0
                {
                    break;
                }
                context.sleep(Duration::from_secs(1)).await;
            }
            assert!(
                indexer
                    .block_upload_completed
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0
            );
        });
    }

    #[test_traced]
    fn test_drainer_waits_for_certificate_uploads() {
        let n = 5;
        let required_container = 10;
        let executor = Runner::timed(Duration::from_secs(30));
        executor.start(|mut context| async move {
            let (network, mut oracle) = Network::new(
                context.child("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                    max_peers_per_set: NZUsize!(n as usize),
                    disconnect_on_block: true,
                    tracked_peer_sets: NZUsize!(1),
                },
            );
            network.start();

            let Fixture {
                schemes,
                private_keys,
                participants,
                ..
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, NAMESPACE, n);
            let mut registrations = register_validators(&mut oracle, &participants).await;

            let link = Link {
                latency: Duration::from_millis(10),
                jitter: Duration::from_millis(1),
                success_rate: probability!(1.0),
            };
            link_validators(&mut oracle, &participants, link, None).await;

            // Hold the first few certificate uploads open so finalized queue
            // rows exist while a certificate path for the same digest is still
            // in flight. The block consumer should wait instead of racing them.
            let mut cert_upload_senders = Vec::new();
            let mut cert_upload_waiters = Vec::new();
            for _ in 0..8 {
                let (sender, receiver) = oneshot::channel();
                cert_upload_senders.push(sender);
                cert_upload_waiters.push(receiver);
            }

            let indexer = mocks::Client::new().with_cert_upload_waiters(cert_upload_waiters);

            for (signer, scheme) in private_keys.into_iter().zip(schemes) {
                let registration = registrations.remove(&signer.public_key()).unwrap();
                start_validator(
                    &context,
                    &oracle,
                    &signer,
                    &scheme,
                    registration,
                    Some(indexer.clone()),
                )
                .await;
            }

            // Wait until consensus has finalized enough blocks for the queue to
            // have pending work, while the shared mock still has certificate
            // uploads blocked in flight.
            let mut metrics = String::new();
            for _ in 0..10 {
                metrics = context.encode();
                if indexer.current_cert_upload_inflight() > 0
                    && queue_outstanding(&metrics) > 0
                    && sum_validator_metric::<u64>(&metrics, "_marshal_processed_height", None)
                        >= required_container
                {
                    break;
                }
                context.sleep(Duration::from_secs(1)).await;
            }

            assert!(
                indexer.current_cert_upload_inflight() > 0,
                "expected at least one certificate upload to remain blocked",
            );
            assert!(
                queue_outstanding(&metrics) > 0,
                "expected finalized queue work while certificate uploads were blocked",
            );
            let genesis_digest = Block::genesis().digest();
            let expected_genesis_uploads = n as usize;
            let started_digests = indexer.block_upload_started_digests.lock().clone();
            assert_eq!(
                started_digests.len(),
                expected_genesis_uploads,
                "only genesis should be uploaded as a bare block while certificate uploads are in flight",
            );
            assert!(
                started_digests.iter().all(|digest| *digest == genesis_digest),
                "non-genesis block uploads should wait while certificate uploads are still in flight",
            );

            // Release the blocked certificate uploads and confirm the
            // certificate-bearing paths finish without the block consumer ever
            // needing to step in.
            drop(cert_upload_senders);
            for _ in 0..10 {
                if indexer
                    .finalization_seen
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    break;
                }
                context.sleep(Duration::from_secs(1)).await;
            }

            assert!(indexer
                .finalization_seen
                .load(std::sync::atomic::Ordering::Relaxed));
            let started_digests = indexer.block_upload_started_digests.lock().clone();
            assert_eq!(
                started_digests.len(),
                expected_genesis_uploads,
                "only genesis should be uploaded as a bare block when certificate uploads eventually succeed",
            );
            assert!(
                started_digests.iter().all(|digest| *digest == genesis_digest),
                "non-genesis block uploads should remain idle when certificate uploads eventually succeed",
            );
        });
    }

    #[test_traced]
    fn test_drainer_uploads_in_parallel() {
        let n = 5;
        let executor = Runner::timed(Duration::from_secs(30));
        executor.start(|mut context| async move {
            let (network, mut oracle) = Network::new(
                context.child("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                    max_peers_per_set: NZUsize!(n as usize),
                    disconnect_on_block: true,
                    tracked_peer_sets: NZUsize!(1),
                },
            );
            network.start();

            // Use the normal validator topology so the only source of
            // concurrency is the consumer itself.
            let Fixture {
                schemes,
                private_keys,
                participants,
                ..
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, NAMESPACE, n);
            let mut registrations = register_validators(&mut oracle, &participants).await;

            let link = Link {
                latency: Duration::from_millis(10),
                jitter: Duration::from_millis(1),
                success_rate: probability!(1.0),
            };
            link_validators(&mut oracle, &participants, link, None).await;

            // Hold the first two block uploads open so we can observe the
            // consumer's parallelism before any upload completes.
            let (release_first, wait_first) = oneshot::channel();
            let (release_second, wait_second) = oneshot::channel();

            let indexer = mocks::Client::new()
                .with_fail_certs()
                .with_block_upload_waiters(vec![wait_first, wait_second]);

            for (signer, scheme) in private_keys.into_iter().zip(schemes) {
                let registration = registrations.remove(&signer.public_key()).unwrap();
                start_validator(
                    &context,
                    &oracle,
                    &signer,
                    &scheme,
                    registration,
                    Some(indexer.clone()),
                )
                .await;
            }

            // Wait until the consumer starts at least two uploads concurrently.
            for _ in 0..10 {
                if indexer
                    .block_upload_started
                    .load(std::sync::atomic::Ordering::SeqCst)
                    >= 2
                {
                    break;
                }
                context.sleep(Duration::from_secs(1)).await;
            }

            assert!(
                indexer
                    .block_upload_started
                    .load(std::sync::atomic::Ordering::SeqCst)
                    >= 2,
                "consumer never started a second block upload while the first was blocked",
            );
            assert!(
                indexer
                    .block_upload_max_inflight
                    .load(std::sync::atomic::Ordering::SeqCst)
                    >= 2,
                "consumer never had multiple block uploads in flight",
            );

            // The queue metrics should reflect the blocked in-flight uploads.
            let mut metrics = String::new();
            for _ in 0..10 {
                metrics = context.encode();
                if queue_held(&metrics) >= 2 {
                    break;
                }
                context.sleep(Duration::from_secs(1)).await;
            }

            let queue_held = queue_held(&metrics);
            assert!(
                queue_held >= 2,
                "queue next-floor metrics never reflected parallel consumer uploads",
            );
            assert!(
                queue_held as usize >= indexer.current_block_upload_inflight(),
                "queue next-floor should be >= mock inflight (may include un-reaped completions)",
            );
            let queue_depth = queue_outstanding(&metrics);
            assert!(
                queue_depth >= queue_held,
                "queue depth should include uploads currently in flight",
            );

            // Allow both blocked uploads to finish so the consumer can retire
            // their queue entries.
            let _ = release_first.send(());
            let _ = release_second.send(());

            for _ in 0..10 {
                if indexer
                    .block_upload_completed
                    .load(std::sync::atomic::Ordering::SeqCst)
                    >= 2
                {
                    break;
                }
                context.sleep(Duration::from_secs(1)).await;
            }

            assert!(
                indexer
                    .block_upload_completed
                    .load(std::sync::atomic::Ordering::SeqCst)
                    >= 2,
                "consumer did not complete the blocked uploads after release",
            );

            let metrics = context.encode();
            let queue_upload_success = sum_validator_metric::<u64>(
                &metrics,
                "_indexer_consumer_uploads_total",
                Some("status=\"success\""),
            );
            assert_eq!(
                queue_upload_success,
                indexer
                    .block_upload_completed
                    .load(std::sync::atomic::Ordering::SeqCst) as u64,
                "queue success counter did not match completed consumer uploads",
            );
            let queue_upload_failure = sum_validator_metric::<u64>(
                &metrics,
                "_indexer_consumer_uploads_total",
                Some("status=\"failure\""),
            );
            assert_eq!(
                queue_upload_failure, 0,
                "queue failure counter should stay at zero when block uploads succeed",
            );
        });
    }

    #[test_traced]
    fn test_drainer_replays_inflight_uploads_after_restart() {
        let n = 5;
        let mut rng = StdRng::seed_from_u64(7);
        let fixture = bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, n);

        // Keep these senders alive for the duration of the first run so the
        // corresponding block uploads remain in flight until shutdown.
        let mut blocked_senders = Vec::new();
        let mut blocked_waiters = Vec::new();
        for _ in 0..16 {
            let (sender, receiver) = oneshot::channel();
            blocked_senders.push(sender);
            blocked_waiters.push(receiver);
        }

        // Use one mock that keeps block uploads blocked during the first run, and
        // a second mock that still rejects cert uploads but lets replayed block
        // uploads complete during recovery.
        let blocked_indexer = mocks::Client::new()
            .with_fail_certs()
            .with_block_upload_waiters(blocked_waiters);
        let recovery_indexer = mocks::Client::new().with_fail_certs();

        let Fixture {
            schemes,
            private_keys,
            participants,
            ..
        } = fixture.clone();
        let first_run_indexer = blocked_indexer.clone();
        let first_run = move |context: deterministic::Context| {
            let indexer = first_run_indexer.clone();
            async move {
                let (network, mut oracle) = Network::new(
                    context.child("network"),
                    simulated::Config {
                        max_size: 1024 * 1024,
                        max_peers_per_set: NZUsize!(n as usize),
                        disconnect_on_block: true,
                        tracked_peer_sets: NZUsize!(1),
                    },
                );
                network.start();

                // Rebuild the original validator set so the durable queue state
                // we recover later comes from a realistic multi-validator run.
                let mut registrations = register_validators(&mut oracle, &participants).await;

                let link = Link {
                    latency: Duration::from_millis(10),
                    jitter: Duration::from_millis(1),
                    success_rate: probability!(1.0),
                };
                link_validators(&mut oracle, &participants, link, None).await;

                for (signer, scheme) in private_keys.into_iter().zip(schemes) {
                    let registration = registrations.remove(&signer.public_key()).unwrap();
                    start_validator(
                        &context,
                        &oracle,
                        &signer,
                        &scheme,
                        registration,
                        Some(indexer.clone()),
                    )
                    .await;
                }

                // Wait until the consumer has definitely started blocked uploads
                // before forcing recovery.
                for _ in 0..10 {
                    if indexer
                        .block_upload_started
                        .load(std::sync::atomic::Ordering::SeqCst)
                        >= 2
                    {
                        break;
                    }
                    context.sleep(Duration::from_secs(1)).await;
                }

                assert!(
                    indexer
                        .block_upload_started
                        .load(std::sync::atomic::Ordering::SeqCst)
                        >= 2,
                    "consumer never had multiple uploads in flight before restart",
                );

                // Confirm the queue still considers those uploads in flight at
                // the moment the first run shuts down.
                let mut metrics = String::new();
                for _ in 0..10 {
                    metrics = context.encode();
                    if queue_held(&metrics) >= 2 {
                        break;
                    }
                    context.sleep(Duration::from_secs(1)).await;
                }

                let queue_held = queue_held(&metrics);
                assert!(
                    queue_held >= 2,
                    "queue next-floor metrics never reflected the blocked uploads before restart",
                );
                assert!(
                    queue_held as usize >= indexer.current_block_upload_inflight(),
                    "queue next-floor should be >= mock inflight before restart (may include occupied consumer slots that have not reached block upload yet)",
                );
                assert!(
                    queue_outstanding(&metrics) >= queue_held,
                    "queue depth should include blocked uploads before restart",
                );

                let started_digests = indexer.block_upload_started_digests.lock().clone();
                let blocked_digests = started_digests[..2].to_vec();
                let completed_digests = indexer.block_upload_completed_digests.lock().clone();
                assert!(
                    blocked_digests
                        .iter()
                        .all(|digest| !completed_digests.contains(digest)),
                    "blocked uploads should remain in flight before shutdown",
                );

                false
            }
        };

        // The blocked uploads keep the first run from draining cleanly, so the
        // runtime should hand us a recoverable checkpoint instead of completion.
        let (complete, checkpoint) =
            Runner::timed(Duration::from_secs(30)).start_and_recover(first_run);
        assert!(!complete);

        let blocked_digests = blocked_indexer.block_upload_started_digests.lock().clone();
        assert!(
            blocked_digests.len() >= 2,
            "expected to capture at least two blocked consumer uploads",
        );
        let expected_digests = blocked_digests[..2].to_vec();

        // Release the original waiters before restart so replayed uploads are
        // free to complete in the recovery run.
        drop(blocked_senders);

        let Fixture {
            schemes,
            private_keys,
            participants,
            ..
        } = fixture;
        let second_run_indexer = recovery_indexer.clone();
        let expected_digests_for_recovery = expected_digests.clone();
        let second_run = move |context: deterministic::Context| async move {
            let indexer = second_run_indexer.clone();
            let expected_digests = expected_digests_for_recovery.clone();
            let (network, mut oracle) = Network::new(
                context.child("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                    max_peers_per_set: NZUsize!(n as usize),
                    disconnect_on_block: true,
                    tracked_peer_sets: NZUsize!(1),
                },
            );
            network.start();

            let mut registrations = register_validators(&mut oracle, &participants).await;

            // Do not relink validators on restart. Any successful block
            // uploads in this run must therefore come from replaying the
            // durable queue and restored marshal state.
            for (signer, scheme) in private_keys.into_iter().zip(schemes) {
                let registration = registrations.remove(&signer.public_key()).unwrap();
                start_validator(
                    &context,
                    &oracle,
                    &signer,
                    &scheme,
                    registration,
                    Some(indexer.clone()),
                )
                .await;
            }

            // The recovery run should replay the previously blocked digests
            // from durable state, even without fresh network activity.
            for _ in 0..10 {
                let completed_digests = indexer.block_upload_completed_digests.lock().clone();
                if expected_digests
                    .iter()
                    .all(|digest| completed_digests.contains(digest))
                {
                    break;
                }
                context.sleep(Duration::from_secs(1)).await;
            }

            let completed_digests = indexer.block_upload_completed_digests.lock().clone();
            assert!(
                expected_digests
                    .iter()
                    .all(|digest| completed_digests.contains(digest)),
                "consumer did not replay the blocked in-flight uploads after restart",
            );

            // After replay succeeds, the durable queue should drain completely.
            let mut metrics = String::new();
            for _ in 0..10 {
                metrics = context.encode();
                if queue_outstanding(&metrics) == 0 && queue_held(&metrics) == 0 {
                    break;
                }
                context.sleep(Duration::from_secs(1)).await;
            }

            assert_eq!(
                queue_outstanding(&metrics),
                0,
                "queue depth metric should return to zero after replay drains the durable queue",
            );
            assert_eq!(
                queue_held(&metrics),
                0,
                "queue next-floor should return to zero after replay completes",
            );
            let queue_upload_success = sum_validator_metric::<u64>(
                &metrics,
                "_indexer_consumer_uploads_total",
                Some("status=\"success\""),
            );
            assert_eq!(
                queue_upload_success,
                indexer
                    .block_upload_completed
                    .load(std::sync::atomic::Ordering::SeqCst) as u64,
                "queue success counter did not match replayed consumer uploads",
            );
            let queue_upload_failure = sum_validator_metric::<u64>(
                &metrics,
                "_indexer_consumer_uploads_total",
                Some("status=\"failure\""),
            );
            assert_eq!(
                queue_upload_failure, 0,
                "queue failure counter should stay at zero while replayed block uploads succeed",
            );

            true
        };

        let (complete, _) = Runner::from(checkpoint).start_and_recover(second_run);
        assert!(complete);

        let completed_digests = recovery_indexer
            .block_upload_completed_digests
            .lock()
            .clone();
        for digest in expected_digests {
            assert!(
                completed_digests.contains(&digest),
                "expected blocked digest to be replayed after restart",
            );
        }
    }

    #[test]
    fn test_finalized_entry_codec() {
        use commonware_codec::{DecodeExt, Encode};
        use commonware_cryptography::{Hasher, Sha256};
        use indexer::Entry;

        let digest = Sha256::hash(&[b"test block"]);
        let entry = Entry { height: 42, digest };

        let encoded = entry.encode();
        let decoded = Entry::decode(encoded.as_ref()).unwrap();
        assert_eq!(decoded.height, 42);
        assert_eq!(decoded.digest, digest);

        assert_eq!(encoded.len(), <Entry as commonware_codec::FixedSize>::SIZE);
    }
}
