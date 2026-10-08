//! Deterministic multi-validator simulations shared by the classical and post-quantum test suites.

use crate::{
    engine::{self, Engine},
    indexer::mocks,
    DEFAULT_BACKFILLER_MAX_ACTIVE, DEFAULT_BACKFILLER_RETRY_MS,
};
use alto_types::{PrivateKey, PublicKey};
use commonware_consensus::{marshal, simplex::elector, types::ViewDelta};
use commonware_cryptography::Signer;
use commonware_p2p::{
    simulated::{self, Link, Network, Oracle, Receiver, Sender},
    Manager,
};
use commonware_parallel::Sequential;
use commonware_runtime::{
    deterministic::{self, Runner},
    Clock, Metrics, Runner as _, Supervisor as _,
};
use commonware_utils::{ordered::Set, NZUsize, NZU32};
use governor::Quota;
use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

/// Limit the freezer table size to 1MB because the deterministic runtime stores
/// everything in RAM.
pub(crate) const FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(14); // 1MB

/// (Effectively) unlimited quota for tests.
pub(crate) const TEST_QUOTA: Quota = Quota::per_second(NZU32!(u32::MAX));

/// Proposal delay of every simulated validator and the term length of stable leaders.
pub(crate) const PROPOSAL_DELAY_MS: u64 = 10;
pub(crate) const STABLE_LEADER_TERM_LENGTH: NonZeroU32 = NZU32!(1_000);

/// Registers all validators using the oracle.
pub(crate) async fn register_validators(
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
        let (recovered_sender, recovered_receiver) = oracle.register(1, TEST_QUOTA).await.unwrap();
        let (resolver_sender, resolver_receiver) = oracle.register(2, TEST_QUOTA).await.unwrap();
        let (broadcast_sender, broadcast_receiver) = oracle.register(3, TEST_QUOTA).await.unwrap();
        let (backfill_sender, backfill_receiver) = oracle.register(4, TEST_QUOTA).await.unwrap();
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
pub(crate) async fn link_validators(
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

pub(crate) fn validator_metric_sample(line: &str) -> Option<(&str, Option<&str>, &str)> {
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

pub(crate) type Registration = (
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
pub(crate) struct ValidatorConfig {
    pub(crate) leader_timeout: Duration,
    pub(crate) certification_timeout: Duration,
    pub(crate) block_size: u32,
    pub(crate) backfiller_max_active: NonZeroUsize,
    pub(crate) backfiller_retry: Duration,
    pub(crate) indexer: Option<mocks::Client>,
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
pub(crate) fn stable_elector() -> engine::StableElector {
    engine::stable_elector(STABLE_LEADER_TERM_LENGTH, 48)
}

/// Starts a validator with an explicit certificate scheme and leader election policy.
pub(crate) async fn start_validator_with<CS: alto_types::Scheme, L: elector::Config<CS>>(
    context: &deterministic::Context,
    oracle: &Oracle<PublicKey, deterministic::Context>,
    signer: &PrivateKey,
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

pub(crate) async fn poll_until_height(
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

/// Runs `n` fully linked stable-leader validators until one processes height `required` and
/// returns the auditor state, which is identical for runs with the same seed.
///
/// `fixture` returns each validator's identity key with its consensus scheme, sorted by key.
pub(crate) fn all_online<CS: alto_types::Scheme>(
    n: u32,
    seed: u64,
    link: Link,
    required: u64,
    fixture: impl FnOnce(&mut deterministic::Context, u32) -> Vec<(PrivateKey, CS)> + Send + 'static,
) -> String {
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

        let validators = fixture(&mut context, n);
        let participants: Vec<_> = validators
            .iter()
            .map(|(signer, _)| signer.public_key())
            .collect();
        let mut registrations = register_validators(&mut oracle, &participants).await;

        link_validators(&mut oracle, &participants, link, None).await;

        for (signer, scheme) in &validators {
            let registration = registrations.remove(&signer.public_key()).unwrap();
            start_validator_with(
                &context,
                &oracle,
                signer,
                scheme,
                stable_elector(),
                registration,
                ValidatorConfig::default(),
            )
            .await;
        }

        poll_until_height(&context, &oracle, required).await;
        context.auditor().state()
    })
}
