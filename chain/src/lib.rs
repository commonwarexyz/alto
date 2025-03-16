use std::future::Future;

use bytes::Bytes;
use commonware_cryptography::bls12381;
use serde::{Deserialize, Serialize};

pub mod actors;
pub mod engine;

/// Trait for interacting with an indexer.
pub trait Indexer: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Create a new indexer with the given URI and public key.
    fn new(uri: &str, public: bls12381::PublicKey) -> Self;

    /// Upload a seed to the indexer.
    fn seed_upload(&self, seed: Bytes) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Upload a notarization to the indexer.
    fn notarization_upload(
        &self,
        notarized: Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Upload a finalization to the indexer.
    fn finalization_upload(
        &self,
        finalized: Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl Indexer for alto_client::Client {
    type Error = alto_client::Error;
    fn new(uri: &str, public: bls12381::PublicKey) -> Self {
        Self::new(uri, public)
    }

    fn seed_upload(
        &self,
        seed: bytes::Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.seed_upload(seed)
    }

    fn notarization_upload(
        &self,
        notarization: bytes::Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.notarization_upload(notarization)
    }

    fn finalization_upload(
        &self,
        finalization: bytes::Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.finalization_upload(finalization)
    }
}

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub private_key: String,
    pub share: String,
    pub identity: String,

    pub port: u16,
    pub directory: String,
    pub worker_threads: usize,
    pub log_level: String,

    pub allowed_peers: Vec<String>,
    pub bootstrappers: Vec<String>,

    pub message_backlog: usize,
    pub mailbox_size: usize,

    pub indexer: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alto_types::{Finalized, Notarized, Seed};
    use bls12381::primitives::poly;
    use commonware_cryptography::{bls12381::dkg::ops, ed25519::PublicKey, Ed25519, Scheme};
    use commonware_macros::test_traced;
    use commonware_p2p::simulated::{self, Link, Network, Oracle, Receiver, Sender};
    use commonware_runtime::{
        deterministic::{self, Executor},
        Clock, Metrics, Runner, Spawner,
    };
    use commonware_utils::quorum;
    use engine::{Config, Engine};
    use governor::Quota;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use std::{
        collections::{HashMap, HashSet},
        num::NonZeroU32,
        sync::{Arc, Mutex},
    };
    use std::{sync::atomic::AtomicBool, time::Duration};
    use tracing::info;

    /// MockIndexer is a simple indexer implementation for testing.
    #[derive(Clone)]
    struct MockIndexer {
        public: bls12381::PublicKey,

        seed_seen: Arc<AtomicBool>,
        notarization_seen: Arc<AtomicBool>,
        finalization_seen: Arc<AtomicBool>,
    }

    impl Indexer for MockIndexer {
        type Error = std::io::Error;

        fn new(_: &str, public: bls12381::PublicKey) -> Self {
            MockIndexer {
                public,
                seed_seen: Arc::new(AtomicBool::new(false)),
                notarization_seen: Arc::new(AtomicBool::new(false)),
                finalization_seen: Arc::new(AtomicBool::new(false)),
            }
        }

        async fn seed_upload(&self, seed: Bytes) -> Result<(), Self::Error> {
            Seed::deserialize(Some(&self.public), &seed).unwrap();
            self.seed_seen
                .store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        async fn notarization_upload(&self, notarized: Bytes) -> Result<(), Self::Error> {
            Notarized::deserialize(Some(&self.public), &notarized).unwrap();
            self.notarization_seen
                .store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        async fn finalization_upload(&self, finalized: Bytes) -> Result<(), Self::Error> {
            Finalized::deserialize(Some(&self.public), &finalized).unwrap();
            self.finalization_seen
                .store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    /// Registers all validators using the oracle.
    async fn register_validators(
        oracle: &mut Oracle<PublicKey>,
        validators: &[PublicKey],
    ) -> HashMap<
        PublicKey,
        (
            (Sender<PublicKey>, Receiver<PublicKey>),
            (Sender<PublicKey>, Receiver<PublicKey>),
            (Sender<PublicKey>, Receiver<PublicKey>),
            (Sender<PublicKey>, Receiver<PublicKey>),
        ),
    > {
        let mut registrations = HashMap::new();
        for validator in validators.iter() {
            let (voter_sender, voter_receiver) =
                oracle.register(validator.clone(), 0).await.unwrap();
            let (resolver_sender, resolver_receiver) =
                oracle.register(validator.clone(), 1).await.unwrap();
            let (broadcast_sender, broadcast_receiver) =
                oracle.register(validator.clone(), 2).await.unwrap();
            let (backfill_sender, backfill_receiver) =
                oracle.register(validator.clone(), 3).await.unwrap();
            registrations.insert(
                validator.clone(),
                (
                    (voter_sender, voter_receiver),
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
        oracle: &mut Oracle<PublicKey>,
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

    fn all_online(seed: u64, link: Link) -> String {
        // Create context
        let n = 5;
        let threshold = quorum(n).unwrap();
        let required_container = 10;
        let cfg = deterministic::Config {
            seed,
            timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        };
        let (executor, mut context, auditor) = Executor::init(cfg);
        executor.start(async move {
            // Create simulated network
            let (network, mut oracle) = Network::new(
                context.with_label("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                },
            );

            // Start network
            network.start();

            // Register participants
            let mut schemes = Vec::new();
            let mut validators = Vec::new();
            for i in 0..n {
                let scheme = Ed25519::from_seed(i as u64);
                let pk = scheme.public_key();
                schemes.push(scheme);
                validators.push(pk);
            }
            validators.sort();
            schemes.sort_by_key(|s| s.public_key());
            let mut registrations = register_validators(&mut oracle, &validators).await;

            // Link all validators
            link_validators(&mut oracle, &validators, link, None).await;

            // Derive threshold
            let (public, shares) = ops::generate_shares(&mut context, None, n, threshold);

            // Create instances
            let mut public_keys = HashSet::new();
            for (idx, scheme) in schemes.into_iter().enumerate() {
                // Create scheme context
                let public_key = scheme.public_key();
                public_keys.insert(public_key.clone());

                // Configure engine
                let uid = format!("validator-{}", public_key);
                let config: Config<MockIndexer> = engine::Config {
                    partition_prefix: uid.clone(),
                    signer: scheme,
                    identity: public.clone(),
                    share: shares[idx],
                    participants: validators.clone(),
                    mailbox_size: 1024,
                    backfill_quota: Quota::per_second(NonZeroU32::new(10).unwrap()),
                    leader_timeout: Duration::from_secs(1),
                    notarization_timeout: Duration::from_secs(2),
                    nullify_retry: Duration::from_secs(10),
                    fetch_timeout: Duration::from_secs(1),
                    activity_timeout: 10,
                    skip_timeout: 5,
                    max_fetch_count: 10,
                    max_fetch_size: 1024 * 512,
                    fetch_concurrent: 10,
                    fetch_rate_per_peer: Quota::per_second(NonZeroU32::new(10).unwrap()),
                    indexer: None,
                };
                let engine = Engine::new(context.with_label(&uid), config).await;

                // Get networking
                let (voter, resolver, broadcast, backfill) =
                    registrations.remove(&public_key).unwrap();

                // Start engine
                engine.start(voter, resolver, broadcast, backfill);
            }

            // Poll metrics
            loop {
                let metrics = context.encode();

                // Iterate over all lines
                let mut success = false;
                for line in metrics.lines() {
                    // Ensure it is a metrics line
                    if !line.starts_with("validator-") {
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
                    if metric.ends_with("_syncer_contiguous_height") {
                        let value = value.parse::<u64>().unwrap();
                        if value >= required_container {
                            success = true;
                            break;
                        }
                    }
                }
                if !success {
                    break;
                }

                // Still waiting for all validators to complete
                context.sleep(Duration::from_secs(1)).await;
            }
        });
        auditor.state()
    }

    #[test_traced]
    fn test_good_links() {
        let link = Link {
            latency: 10.0,
            jitter: 1.0,
            success_rate: 1.0,
        };
        for seed in 0..5 {
            let state = all_online(seed, link.clone());
            assert_eq!(state, all_online(seed, link.clone()));
        }
    }

    #[test_traced]
    fn test_bad_links() {
        let link = Link {
            latency: 200.0,
            jitter: 150.0,
            success_rate: 0.75,
        };
        for seed in 0..5 {
            let state = all_online(seed, link.clone());
            assert_eq!(state, all_online(seed, link.clone()));
        }
    }

    #[test_traced]
    fn test_backfill() {
        // Create context
        let n = 5;
        let threshold = quorum(n).unwrap();
        let initial_container_required = 10;
        let final_container_required = 20;
        let (executor, mut context, _) = Executor::timed(Duration::from_secs(30));
        executor.start(async move {
            // Create simulated network
            let (network, mut oracle) = Network::new(
                context.with_label("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                },
            );

            // Start network
            network.start();

            // Register participants
            let mut schemes = Vec::new();
            let mut validators = Vec::new();
            for i in 0..n {
                let scheme = Ed25519::from_seed(i as u64);
                let pk = scheme.public_key();
                schemes.push(scheme);
                validators.push(pk);
            }
            validators.sort();
            schemes.sort_by_key(|s| s.public_key());
            let mut registrations = register_validators(&mut oracle, &validators).await;

            // Link all validators (except 0)
            let link = Link {
                latency: 10.0,
                jitter: 1.0,
                success_rate: 1.0,
            };
            link_validators(
                &mut oracle,
                &validators,
                link.clone(),
                Some(|_, i, j| ![i, j].contains(&0usize)),
            )
            .await;

            // Derive threshold
            let (public, shares) = ops::generate_shares(&mut context, None, n, threshold);

            // Create instances
            for (idx, scheme) in schemes.iter().enumerate() {
                // Skip first
                if idx == 0 {
                    continue;
                }

                // Configure engine
                let public_key = scheme.public_key();
                let uid = format!("validator-{}", public_key);
                let config: Config<MockIndexer> = engine::Config {
                    partition_prefix: uid.clone(),
                    signer: scheme.clone(),
                    identity: public.clone(),
                    share: shares[idx],
                    participants: validators.clone(),
                    mailbox_size: 1024,
                    backfill_quota: Quota::per_second(NonZeroU32::new(10).unwrap()),
                    leader_timeout: Duration::from_secs(1),
                    notarization_timeout: Duration::from_secs(2),
                    nullify_retry: Duration::from_secs(10),
                    fetch_timeout: Duration::from_secs(1),
                    activity_timeout: 10,
                    skip_timeout: 5,
                    max_fetch_count: 10,
                    max_fetch_size: 1024 * 512,
                    fetch_concurrent: 10,
                    fetch_rate_per_peer: Quota::per_second(NonZeroU32::new(10).unwrap()),
                    indexer: None,
                };
                let engine = Engine::new(context.with_label(&uid), config).await;

                // Get networking
                let (voter, resolver, broadcast, backfill) =
                    registrations.remove(&public_key).unwrap();

                // Start engine
                engine.start(voter, resolver, broadcast, backfill);
            }

            // Poll metrics
            loop {
                let metrics = context.encode();

                // Iterate over all lines
                let mut success = true;
                for line in metrics.lines() {
                    // Ensure it is a metrics line
                    if !line.starts_with("validator-") {
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
                    if metric.ends_with("_syncer_contiguous_height") {
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
                &validators,
                link,
                Some(|_, i, j| [i, j].contains(&0usize) && ![i, j].contains(&1usize)),
            )
            .await;

            // Configure engine
            let scheme = schemes[0].clone();
            let share = shares[0];
            let public_key = scheme.public_key();
            let uid = format!("validator-{}", public_key);
            let config: Config<MockIndexer> = engine::Config {
                partition_prefix: uid.clone(),
                signer: scheme.clone(),
                identity: public.clone(),
                share,
                participants: validators.clone(),
                mailbox_size: 1024,
                backfill_quota: Quota::per_second(NonZeroU32::new(10).unwrap()),
                leader_timeout: Duration::from_secs(1),
                notarization_timeout: Duration::from_secs(2),
                nullify_retry: Duration::from_secs(10),
                fetch_timeout: Duration::from_secs(1),
                activity_timeout: 10,
                skip_timeout: 5,
                max_fetch_count: 10,
                max_fetch_size: 1024 * 512,
                fetch_concurrent: 10,
                fetch_rate_per_peer: Quota::per_second(NonZeroU32::new(10).unwrap()),
                indexer: None,
            };
            let engine = Engine::new(context.with_label(&uid), config).await;

            // Get networking
            let (voter, resolver, broadcast, backfill) = registrations.remove(&public_key).unwrap();

            // Start engine
            engine.start(voter, resolver, broadcast, backfill);

            // Poll metrics
            loop {
                let metrics = context.encode();

                // Iterate over all lines
                let mut success = false;
                for line in metrics.lines() {
                    // Ensure it is a metrics line
                    if !line.starts_with("validator-") {
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
                    if metric.ends_with("_syncer_contiguous_height") {
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
        let threshold = quorum(n).unwrap();
        let required_container = 100;

        // Derive threshold
        let mut rng = StdRng::seed_from_u64(0);
        let (public, shares) = ops::generate_shares(&mut rng, None, n, threshold);

        // Random restarts every x seconds
        let mut runs = 0;
        let done = Arc::new(Mutex::new(false));
        let (mut executor, mut context, _) = Executor::timed(Duration::from_secs(10));
        while !*done.lock().unwrap() {
            runs += 1;
            executor.start({
                let mut context = context.clone();
                let public = public.clone();
                let shares = shares.clone();
                let done = done.clone();
                async move {
                    // Create simulated network
                    let (network, mut oracle) = Network::new(
                        context.with_label("network"),
                        simulated::Config {
                            max_size: 1024 * 1024,
                        },
                    );

                    // Start network
                    network.start();

                    // Register participants
                    let mut schemes = Vec::new();
                    let mut validators = Vec::new();
                    for i in 0..n {
                        let scheme = Ed25519::from_seed(i as u64);
                        let pk = scheme.public_key();
                        schemes.push(scheme);
                        validators.push(pk);
                    }
                    validators.sort();
                    schemes.sort_by_key(|s| s.public_key());
                    let mut registrations = register_validators(&mut oracle, &validators).await;

                    // Link all validators
                    let link = Link {
                        latency: 10.0,
                        jitter: 1.0,
                        success_rate: 1.0,
                    };
                    link_validators(&mut oracle, &validators, link, None).await;

                    // Create instances
                    let mut public_keys = HashSet::new();
                    for (idx, scheme) in schemes.into_iter().enumerate() {
                        // Create scheme context
                        let public_key = scheme.public_key();
                        public_keys.insert(public_key.clone());

                        // Configure engine
                        let uid = format!("validator-{}", public_key);
                        let config: Config<MockIndexer> = engine::Config {
                            partition_prefix: uid.clone(),
                            signer: scheme,
                            identity: public.clone(),
                            share: shares[idx],
                            participants: validators.clone(),
                            mailbox_size: 1024,
                            backfill_quota: Quota::per_second(NonZeroU32::new(10).unwrap()),
                            leader_timeout: Duration::from_secs(1),
                            notarization_timeout: Duration::from_secs(2),
                            nullify_retry: Duration::from_secs(10),
                            fetch_timeout: Duration::from_secs(1),
                            activity_timeout: 10,
                            skip_timeout: 5,
                            max_fetch_count: 10,
                            max_fetch_size: 1024 * 512,
                            fetch_concurrent: 10,
                            fetch_rate_per_peer: Quota::per_second(NonZeroU32::new(10).unwrap()),
                            indexer: None,
                        };
                        let engine = Engine::new(context.with_label(&uid), config).await;

                        // Get networking
                        let (voter, resolver, broadcast, backfill) =
                            registrations.remove(&public_key).unwrap();

                        // Start engine
                        engine.start(voter, resolver, broadcast, backfill);
                    }

                    // Poll metrics
                    context
                        .with_label("metrics")
                        .spawn(move |context| async move {
                            loop {
                                let metrics = context.encode();

                                // Iterate over all lines
                                let mut success = false;
                                for line in metrics.lines() {
                                    // Ensure it is a metrics line
                                    if !line.starts_with("validator-") {
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
                                    if metric.ends_with("_syncer_contiguous_height") {
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
                            *done.lock().unwrap() = true;
                        });

                    // Exit at random points until finished
                    let wait =
                        context.gen_range(Duration::from_millis(10)..Duration::from_millis(1_000));
                    context.sleep(wait).await;
                }
            });

            // Recover context
            (executor, context, _) = context.recover();
        }
        assert!(runs > 1);
        info!(runs, "unclean shutdown recovery worked");
    }

    #[test_traced]
    fn test_indexer() {
        // Create context
        let n = 5;
        let threshold = quorum(n).unwrap();
        let required_container = 10;
        let (executor, mut context, _) = Executor::timed(Duration::from_secs(30));
        executor.start(async move {
            // Create simulated network
            let (network, mut oracle) = Network::new(
                context.with_label("network"),
                simulated::Config {
                    max_size: 1024 * 1024,
                },
            );

            // Start network
            network.start();

            // Register participants
            let mut schemes = Vec::new();
            let mut validators = Vec::new();
            for i in 0..n {
                let scheme = Ed25519::from_seed(i as u64);
                let pk = scheme.public_key();
                schemes.push(scheme);
                validators.push(pk);
            }
            validators.sort();
            schemes.sort_by_key(|s| s.public_key());
            let mut registrations = register_validators(&mut oracle, &validators).await;

            // Link all validators
            let link = Link {
                latency: 10.0,
                jitter: 1.0,
                success_rate: 1.0,
            };
            link_validators(&mut oracle, &validators, link, None).await;

            // Derive threshold
            let (public, shares) = ops::generate_shares(&mut context, None, n, threshold);
            let public_key = *poly::public(&public);

            // Define mock indexer
            let indexer = MockIndexer::new("", public_key.into());

            // Create instances
            let mut public_keys = HashSet::new();
            for (idx, scheme) in schemes.into_iter().enumerate() {
                // Create scheme context
                let public_key = scheme.public_key();
                public_keys.insert(public_key.clone());

                // Configure engine
                let uid = format!("validator-{}", public_key);
                let config: Config<MockIndexer> = engine::Config {
                    partition_prefix: uid.clone(),
                    signer: scheme,
                    identity: public.clone(),
                    share: shares[idx],
                    participants: validators.clone(),
                    mailbox_size: 1024,
                    backfill_quota: Quota::per_second(NonZeroU32::new(10).unwrap()),
                    leader_timeout: Duration::from_secs(1),
                    notarization_timeout: Duration::from_secs(2),
                    nullify_retry: Duration::from_secs(10),
                    fetch_timeout: Duration::from_secs(1),
                    activity_timeout: 10,
                    skip_timeout: 5,
                    max_fetch_count: 10,
                    max_fetch_size: 1024 * 512,
                    fetch_concurrent: 10,
                    fetch_rate_per_peer: Quota::per_second(NonZeroU32::new(10).unwrap()),
                    indexer: Some(indexer.clone()),
                };
                let engine = Engine::new(context.with_label(&uid), config).await;

                // Get networking
                let (voter, resolver, broadcast, backfill) =
                    registrations.remove(&public_key).unwrap();

                // Start engine
                engine.start(voter, resolver, broadcast, backfill);
            }

            // Poll metrics
            loop {
                let metrics = context.encode();

                // Iterate over all lines
                let mut success = false;
                for line in metrics.lines() {
                    // Ensure it is a metrics line
                    if !line.starts_with("validator-") {
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
                    if metric.ends_with("_syncer_contiguous_height") {
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
