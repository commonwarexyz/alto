use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr};

pub mod application;
pub mod engine;
pub mod indexer;
pub mod utils;

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
    pub log_level: String,

    pub local: bool,
    pub allowed_peers: Vec<String>,
    pub bootstrappers: Vec<String>,

    pub message_backlog: usize,
    pub mailbox_size: usize,
    pub deque_size: usize,

    pub indexer: Option<String>,
}

/// A list of peers provided when a validator is run locally.
///
/// When run remotely, [commonware_deployer::ec2::Hosts] is used instead.
#[derive(Deserialize, Serialize)]
pub struct Peers {
    pub addresses: HashMap<String, SocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_consensus::{marshal, simplex::scheme::bls12381_threshold, types::ViewDelta};
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig, certificate::mocks::Fixture, ed25519::PublicKey,
        Signer,
    };
    use commonware_macros::{select, test_traced};
    use commonware_p2p::{
        simulated::{self, Link, Network, Oracle, Receiver, Sender},
        Manager,
    };
    use commonware_runtime::{
        deterministic::{self, Runner},
        Clock, Metrics, Runner as _, Spawner,
    };
    use commonware_utils::{ordered::Set, NZU32};
    use engine::{Config, Engine};
    use governor::Quota;
    use indexer::{Indexer, Mock};
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
            .update(0, Set::from_iter_dedup(validators.iter().cloned()))
            .await;
        let mut registrations = HashMap::new();
        for validator in validators.iter() {
            let mut oracle = oracle.control(validator.clone());
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
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, n);
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
                let config: Config<_, Mock> = engine::Config {
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
                };
                let engine = Engine::new(context.with_label(&uid), config).await;

                // Get networking
                let (pending, recovered, resolver, broadcast, backfill) =
                    registrations.remove(&public_key).unwrap();

                // Configure marshal resolver
                let marshal_resolver_cfg = marshal::resolver::p2p::Config {
                    public_key: public_key.clone(),
                    manager: oracle.manager(),
                    blocker: oracle.control(public_key.clone()),
                    mailbox_size: 1024,
                    initial: Duration::from_secs(1),
                    timeout: Duration::from_secs(2),
                    fetch_retry_timeout: Duration::from_millis(100),
                    priority_requests: false,
                    priority_responses: false,
                };

                let marshal_resolver =
                    marshal::resolver::p2p::init(&context, marshal_resolver_cfg, backfill);

                // Start engine
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
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, n);
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
                let config: Config<_, Mock> = engine::Config {
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
                };
                let engine = Engine::new(context.with_label(&uid), config).await;

                // Get networking
                let (pending, recovered, resolver, broadcast, backfill) =
                    registrations.remove(&public_key).unwrap();

                // Configure marshal resolver
                let marshal_resolver_cfg = marshal::resolver::p2p::Config {
                    public_key: public_key.clone(),
                    manager: oracle.manager(),
                    blocker: oracle.control(public_key.clone()),
                    mailbox_size: 1024,
                    initial: Duration::from_secs(1),
                    timeout: Duration::from_secs(2),
                    fetch_retry_timeout: Duration::from_millis(100),
                    priority_requests: false,
                    priority_responses: false,
                };

                let marshal_resolver =
                    marshal::resolver::p2p::init(&context, marshal_resolver_cfg, backfill);

                // Start engine
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
            let config: Config<_, Mock> = engine::Config {
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
            };
            let engine = Engine::new(context.with_label(&uid), config).await;

            // Get networking
            let (pending, recovered, resolver, broadcast, backfill) =
                registrations.remove(&public_key).unwrap();

            // Configure marshal resolver
            let marshal_resolver_cfg = marshal::resolver::p2p::Config {
                public_key: public_key.clone(),
                manager: oracle.manager(),
                blocker: oracle.control(public_key.clone()),
                mailbox_size: 1024,
                initial: Duration::from_secs(1),
                timeout: Duration::from_secs(2),
                fetch_retry_timeout: Duration::from_millis(100),
                priority_requests: false,
                priority_responses: false,
            };

            let marshal_resolver =
                marshal::resolver::p2p::init(&context, marshal_resolver_cfg, backfill);

            // Start engine
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
        let fixture = bls12381_threshold::fixture::<MinSig, _>(&mut rng, n);

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
                    let config: Config<_, Mock> = engine::Config {
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
                    };
                    let engine = Engine::new(context.with_label(&uid), config).await;

                    // Get networking
                    let (pending, recovered, resolver, broadcast, backfill) =
                        registrations.remove(&public_key).unwrap();

                    // Configure marshal resolver
                    let marshal_resolver_cfg = marshal::resolver::p2p::Config {
                        public_key: public_key.clone(),
                        manager: oracle.manager(),
                        blocker: oracle.control(public_key.clone()),
                        mailbox_size: 1024,
                        initial: Duration::from_secs(1),
                        timeout: Duration::from_secs(2),
                        fetch_retry_timeout: Duration::from_millis(100),
                        priority_requests: false,
                        priority_responses: false,
                    };

                    let marshal_resolver =
                        marshal::resolver::p2p::init(&context, marshal_resolver_cfg, backfill);

                    // Start engine
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
            } = bls12381_threshold::fixture::<MinSig, _>(&mut context, n);
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
            let indexer = Mock::new("", identity);

            // Create instances
            let mut public_keys = HashSet::new();
            for (signer, scheme) in private_keys.into_iter().zip(schemes) {
                // Create signer context
                let public_key = signer.public_key();
                public_keys.insert(public_key.clone());

                // Configure engine
                let uid = format!("validator_{public_key}");
                let config: Config<_, Mock> = engine::Config {
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
                };
                let engine = Engine::new(context.with_label(&uid), config).await;

                // Get networking
                let (pending, recovered, resolver, broadcast, backfill) =
                    registrations.remove(&public_key).unwrap();

                // Configure marshal resolver
                let marshal_resolver_cfg = marshal::resolver::p2p::Config {
                    public_key: public_key.clone(),
                    manager: oracle.manager(),
                    blocker: oracle.control(public_key.clone()),
                    mailbox_size: 1024,
                    initial: Duration::from_secs(1),
                    timeout: Duration::from_secs(2),
                    fetch_retry_timeout: Duration::from_millis(100),
                    priority_requests: false,
                    priority_responses: false,
                };

                let marshal_resolver =
                    marshal::resolver::p2p::init(&context, marshal_resolver_cfg, backfill);

                // Start engine
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
