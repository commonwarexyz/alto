use super::*;
use crate::application::Application;
use alto_types::{StandardScheme, NAMESPACE};
use bytes::Bytes;
use commonware_consensus::{
    marshal::ancestry,
    simplex::{
        elector::RoundRobin,
        scheme::bls12381_threshold::standard,
        types::{Finalization, Finalize, Notarization, Notarize, Nullification, Nullify},
    },
    types::{Epoch, Height, ViewDelta},
    Application as ConsensusApplication, Handoff,
};
use commonware_cryptography::{
    bls12381::primitives::variant::MinSig, certificate::Scheme as _, Digest as _, Sha256,
};
use commonware_parallel::Sequential;
use commonware_runtime::{deterministic, Clock, Runner as _, Supervisor};
use commonware_utils::{non_empty, test_rng, ScriptedRng, SystemTimeExt, NZU32};
use futures::FutureExt;
use rand::CryptoRng;
use std::time::Duration;

struct Harness {
    application: Application<StandardScheme>,
    reporter: Handoffs<StandardScheme>,
    schemes: Vec<StandardScheme>,
    elector: <RoundRobin<Sha256> as Config<StandardScheme>>::Elector,
}

impl Harness {
    fn new(rng: &mut impl CryptoRng, config: RoundRobin<Sha256>) -> Self {
        let fixture = standard::fixture::<MinSig, _>(rng, NAMESPACE, 4);
        let scheme = &fixture.schemes[2];
        let elector = config.clone().build(scheme.participants());
        let reporter = Handoffs::new(scheme, config);
        let application = Application::new(0, 0).with_handoffs(reporter.clone());
        Self {
            application,
            reporter,
            schemes: fixture.schemes,
            elector,
        }
    }

    fn inputs(&self, view: u64) -> (Context, Digested<Block>) {
        let parent_round = Round::new(EPOCH, View::new(view - 1));
        let parent_leader = self.elector.elect(parent_round, ());
        let participants = self.schemes[0].participants();
        let parent = Block::new(
            Context {
                round: parent_round,
                leader: participants[usize::from(parent_leader)].clone(),
                parent: (View::new(view - 2), Digest::EMPTY),
            },
            Digest::EMPTY,
            Height::new(view - 1),
            0,
            Bytes::new(),
        );
        let parent_digest = parent.digest();
        let round = Round::new(EPOCH, View::new(view));
        let leader = self.elector.elect(round, ());
        (
            Context {
                round,
                leader: participants[usize::from(leader)].clone(),
                parent: (parent_round.view(), parent_digest),
            },
            Digested::precomputed(parent, parent_digest),
        )
    }

    async fn prepare(
        &mut self,
        runtime: &deterministic::Context,
        view: u64,
    ) -> Handoff<Digested<Block>> {
        let (context, parent) = self.inputs(view);
        self.application
            .prepare(
                (runtime.child("prepare"), context),
                ancestry::from_iter([parent]),
                (),
            )
            .await
    }

    async fn propose(&mut self, runtime: &deterministic::Context, view: u64) -> Digested<Block> {
        let (context, parent) = self.inputs(view);
        self.application
            .propose(
                (runtime.child("propose"), context),
                ancestry::from_iter([parent]),
                (),
            )
            .await
            .unwrap()
    }

    async fn sample(&mut self, runtime: &deterministic::Context, view: u64) -> Digested<Block> {
        let block = self.propose(runtime, view).await;
        self.reporter.prepared(&block);
        block
    }

    fn nullification(&self, round: Round) -> Activity<StandardScheme> {
        let votes: Vec<_> = self
            .schemes
            .iter()
            .map(|scheme| Nullify::sign::<Digest>(scheme, round).unwrap())
            .collect();
        Activity::Nullification(
            Nullification::from_nullifies(&self.schemes[0], non_empty![@&votes], &Sequential)
                .unwrap(),
        )
    }

    fn certification(&self, block: &Digested<Block>) -> Activity<StandardScheme> {
        let context = &block.inner().context;
        let proposal = Proposal::new(context.round, context.parent.0, block.digest());
        let votes: Vec<_> = self
            .schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        Activity::Certification(
            Notarization::from_notarizes(&self.schemes[0], non_empty![@&votes], &Sequential)
                .unwrap(),
        )
    }

    fn finalization(&self, block: &Digested<Block>) -> Activity<StandardScheme> {
        let context = &block.inner().context;
        let proposal = Proposal::new(context.round, context.parent.0, block.digest());
        let votes: Vec<_> = self
            .schemes
            .iter()
            .map(|scheme| Finalize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        Activity::Finalization(
            Finalization::from_finalizes(&self.schemes[0], non_empty![@&votes], &Sequential)
                .unwrap(),
        )
    }

    async fn fail(&mut self, runtime: &deterministic::Context, view: u64) -> Digested<Block> {
        let block = self.sample(runtime, view).await;
        self.reporter
            .report(self.nullification(block.inner().context.round));
        block
    }
}

#[test]
fn successes_restore_preference_gradually() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let mut h = Harness::new(&mut runtime, RoundRobin::default());
        let mut previous = RATE_SCALE;
        for view in [6, 10, 14, 18] {
            h.fail(&runtime, view).await;
            let rate = h.reporter.state.lock().unwrap().success_rates[1];
            assert!(rate < previous);
            previous = rate;
        }

        for view in [22, 26, 30, 34] {
            let block = h.sample(&runtime, view).await;
            h.reporter.report(h.certification(&block));
            let rate = h.reporter.state.lock().unwrap().success_rates[1];
            assert!(rate > previous);
            assert!(rate - previous <= RATE_SCALE / 8);
            assert!(rate < RATE_SCALE);
            previous = rate;
        }

        h.reporter.state.lock().unwrap().success_rates[1] = RATE_SCALE - 1;
        let block = h.sample(&runtime, 38).await;
        h.reporter.report(h.certification(&block));
        assert_eq!(
            h.reporter.state.lock().unwrap().success_rates[1],
            RATE_SCALE
        );
    });
}

#[test]
fn reactive_parent_cannot_recover_by_cooperating_only_when_we_wait() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let mut h = Harness::new(&mut runtime, RoundRobin::default());
        for recover in [false, true] {
            let mut late_early_votes = 0;
            let mut first_recovery = true;
            for step in 0..256 {
                let view = 6 + 4 * (step + u64::from(recover) * 256);
                let before = h.reporter.state.lock().unwrap().success_rates[1];
                match h.prepare(&runtime, view).await {
                    Handoff::Vote(block) => {
                        // Count the latter half of each phase so the estimate has time to adapt.
                        if step >= 128 {
                            late_early_votes += 1;
                        }
                        if recover {
                            h.reporter.report(h.certification(&block));
                            if first_recovery {
                                let rate = h.reporter.state.lock().unwrap().success_rates[1];
                                assert!(rate > before);
                                assert!(rate < RATE_SCALE / 2);
                                first_recovery = false;
                            }
                        } else {
                            h.reporter
                                .report(h.nullification(block.inner().context.round));
                        }
                    }
                    Handoff::Wait => {
                        let block = h.propose(&runtime, view).await;
                        h.reporter.report(h.certification(&block));
                        assert_eq!(h.reporter.state.lock().unwrap().success_rates[1], before);
                    }
                    Handoff::Stage(_) => panic!("handoffs must either vote early or wait"),
                }
            }
            if recover {
                assert!(
                    late_early_votes > 100,
                    "successful probes must restore early voting"
                );
                assert!(h.reporter.state.lock().unwrap().success_rates[1] > RATE_SCALE * 99 / 100);
            } else {
                assert!(
                    late_early_votes > 0,
                    "early probes must continue after repeated failures"
                );
                assert!(
                    late_early_votes < 24,
                    "failed early proposals must reduce early voting"
                );
                assert!(h.reporter.state.lock().unwrap().success_rates[1] <= RATE_SCALE / 16);
            }
        }
    });
}

#[test]
fn preparation_probability_matches_estimate_with_a_recovery_floor() {
    let h = Harness::new(&mut test_rng(), RoundRobin::default());
    let (context, _) = h.inputs(6);
    assert!(h.reporter.prepare(&context, &mut ScriptedRng::new([])));
    for (rate, threshold) in [(0, 1_u64 << 60), (RATE_SCALE / 2, 1_u64 << 63)] {
        h.reporter.state.lock().unwrap().success_rates[1] = rate;
        assert!(h
            .reporter
            .prepare(&context, &mut ScriptedRng::new([threshold - 1])));
        assert!(!h
            .reporter
            .prepare(&context, &mut ScriptedRng::new([threshold])));
        assert!(h.reporter.state.lock().unwrap().pending.is_none());
    }
}

#[test]
fn only_our_nullification_updates_the_parent_leader() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let mut h = Harness::new(&mut runtime, RoundRobin::default());
        let previous = h.reporter.state.lock().unwrap().success_rates.clone();
        let Handoff::Vote(block) = h.prepare(&runtime, 6).await else {
            panic!("full confidence must select an early proposal");
        };
        let context = &block.inner().context;

        h.reporter
            .report(h.nullification(Round::new(EPOCH, context.parent.0)));
        assert_eq!(h.reporter.state.lock().unwrap().success_rates, previous);
        h.reporter.report(h.nullification(context.round));
        let state = h.reporter.state.lock().unwrap();
        assert_eq!(state.success_rates[1], RATE_SCALE * 7 / 8);
        assert_eq!(state.success_rates[0], previous[0]);
        assert_eq!(state.success_rates[2..], previous[2..]);
    });
}

#[test]
fn only_the_first_own_outcome_updates_a_sample() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let mut h = Harness::new(&mut runtime, RoundRobin::default());
        h.fail(&runtime, 6).await;
        let before = h.reporter.state.lock().unwrap().success_rates.clone();
        let block = h.sample(&runtime, 10).await;
        let round = block.inner().context.round;
        h.reporter.report(Activity::Nullify(
            Nullify::sign::<Digest>(&h.schemes[0], round).unwrap(),
        ));
        let Activity::Certification(notarization) = h.certification(&block) else {
            unreachable!()
        };
        h.reporter.report(Activity::Notarization(notarization));
        h.reporter
            .report(h.nullification(Round::new(Epoch::new(1), round.view())));
        h.reporter
            .report(h.nullification(Round::new(EPOCH, View::new(11))));
        assert_eq!(h.reporter.state.lock().unwrap().success_rates, before);

        h.reporter.report(h.certification(&block));
        let after = h.reporter.state.lock().unwrap().success_rates.clone();
        assert!(after[1] > before[1]);
        h.reporter.report(h.certification(&block));
        h.reporter.report(h.nullification(round));
        assert_eq!(h.reporter.state.lock().unwrap().success_rates, after);

        let failed = h.fail(&runtime, 14).await;
        let after = h.reporter.state.lock().unwrap().success_rates.clone();
        h.reporter
            .report(h.nullification(failed.inner().context.round));
        h.reporter.report(h.certification(&failed));
        h.reporter.report(h.certification(&block));
        assert_eq!(h.reporter.state.lock().unwrap().success_rates, after);
    });
}

#[test]
fn a_replacement_proposal_does_not_count_as_early_success() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let mut h = Harness::new(&mut runtime, RoundRobin::default());
        h.fail(&runtime, 6).await;
        let before = h.reporter.state.lock().unwrap().success_rates.clone();
        let block = h.sample(&runtime, 10).await;
        let (mut context, _) = h.inputs(10);
        let (_, parent) = h.inputs(9);
        context.parent = (parent.inner().context.round.view(), parent.digest());
        let replacement = h
            .application
            .propose(
                (runtime.child("replacement"), context),
                ancestry::from_iter([parent]),
                (),
            )
            .await
            .unwrap();
        h.reporter.report(h.certification(&replacement));
        h.reporter.report(h.certification(&block));
        let state = h.reporter.state.lock().unwrap();
        assert_eq!(state.success_rates, before);
        assert!(state.pending.is_none());
    });
}

#[test]
fn pipelined_preparation_preserves_the_first_unresolved_sample() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let mut h = Harness::new(&mut runtime, RoundRobin::default());
        h.fail(&runtime, 6).await;
        let before = h.reporter.state.lock().unwrap().success_rates[1];
        let block = h.sample(&runtime, 10).await;
        let newer = h.sample(&runtime, 14).await;
        h.reporter.report(h.certification(&block));
        let after = h.reporter.state.lock().unwrap().success_rates[1];
        assert!(after > before);
        h.reporter
            .report(h.nullification(newer.inner().context.round));
        assert_eq!(h.reporter.state.lock().unwrap().success_rates[1], after);
        h.fail(&runtime, 18).await;
        assert!(h.reporter.state.lock().unwrap().success_rates[1] < after);
    });
}

#[test]
fn finalization_can_resolve_an_early_sample_without_local_certification() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let mut h = Harness::new(&mut runtime, RoundRobin::default());
        h.fail(&runtime, 6).await;
        let before = h.reporter.state.lock().unwrap().success_rates[1];
        let block = h.sample(&runtime, 10).await;
        h.reporter.report(h.finalization(&block));
        assert!(h.reporter.state.lock().unwrap().success_rates[1] > before);
    });
}

#[test]
fn higher_finalization_retires_a_sample_and_allows_learning_from_the_next() {
    deterministic::Runner::default().start(|mut runtime| async move {
        for (config, sampled, finalized, next) in [
            (RoundRobin::default(), 6, 7, 10),
            (
                RoundRobin::default().with_term(
                    TermLength::new(NZU32!(4)),
                    Duration::from_secs(12),
                    ViewDelta::new(4),
                ),
                21,
                22,
                37,
            ),
        ] {
            let mut h = Harness::new(&mut runtime, config);
            let stale = h.sample(&runtime, sampled).await;
            let (mut context, _) = h.inputs(finalized);
            context.parent = (stale.inner().context.round.view(), stale.digest());
            let block = h
                .application
                .propose(
                    (runtime.child("finalized"), context),
                    ancestry::from_iter([stale.clone()]),
                    (),
                )
                .await
                .unwrap();
            h.reporter.report(h.finalization(&block));
            assert_eq!(
                h.reporter.state.lock().unwrap().success_rates,
                vec![RATE_SCALE; 4]
            );
            h.reporter.prepared(&stale);
            assert!(h.reporter.state.lock().unwrap().pending.is_none());

            h.fail(&runtime, next).await;
            assert_eq!(
                h.reporter.state.lock().unwrap().success_rates[1],
                RATE_SCALE * 7 / 8,
                "finalization of view {finalized} must retire the sample from view {sampled}"
            );
        }
    });
}

#[test]
fn finalizing_the_parent_preserves_its_childs_pending_sample() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let mut h = Harness::new(&mut runtime, RoundRobin::default());
        h.fail(&runtime, 6).await;
        let before = h.reporter.state.lock().unwrap().success_rates[1];
        let block = h.sample(&runtime, 10).await;
        let (_, parent) = h.inputs(10);
        h.reporter.report(h.finalization(&parent));
        h.reporter.report(h.certification(&block));
        assert!(h.reporter.state.lock().unwrap().success_rates[1] > before);
    });
}

#[test]
fn stable_terms_count_only_term_start_outcomes() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let config = RoundRobin::default().with_term(
            TermLength::new(NZU32!(4)),
            Duration::from_secs(12),
            ViewDelta::new(4),
        );
        let mut h = Harness::new(&mut runtime, config);
        let block = h.sample(&runtime, 21).await;
        h.reporter
            .report(h.nullification(Round::new(EPOCH, View::new(22))));
        assert_eq!(
            h.reporter.state.lock().unwrap().success_rates[1],
            RATE_SCALE
        );
        let (context, _) = h.inputs(22);
        assert!(!h.reporter.prepare(&context, &mut ScriptedRng::new([])));
        h.reporter
            .report(h.nullification(block.inner().context.round));
        assert!(h.reporter.state.lock().unwrap().success_rates[1] < RATE_SCALE);
        let (context, _) = h.inputs(37);
        assert!(!h
            .reporter
            .prepare(&context, &mut ScriptedRng::new([u64::MAX])));
    });
}

#[test]
fn empty_and_canceled_preparations_do_not_create_samples() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let mut h = Harness::new(&mut runtime, RoundRobin::default());
        let (context, _) = h.inputs(6);
        let result = h
            .application
            .prepare(
                (runtime.child("empty"), context.clone()),
                ancestry::from_iter([]),
                (),
            )
            .await;
        assert!(matches!(result, Handoff::Wait));
        h.reporter.report(h.nullification(context.round));

        let (mut context, parent) = h.inputs(10);
        let mut parent = parent.inner().clone();
        parent.timestamp = runtime.current().epoch_millis();
        let digest = parent.digest();
        context.parent.1 = digest;
        h.application = Application::new(500, 0).with_handoffs(h.reporter.clone());
        assert!(h
            .application
            .prepare(
                (runtime.child("canceled"), context.clone()),
                ancestry::from_iter([Digested::precomputed(parent, digest)]),
                (),
            )
            .now_or_never()
            .is_none());
        h.reporter.report(h.nullification(context.round));
        let state = h.reporter.state.lock().unwrap();
        assert_eq!(state.success_rates, vec![RATE_SCALE; 4]);
        assert!(state.pending.is_none());
    });
}

#[test]
fn late_preparation_cannot_reopen_a_resolved_view() {
    deterministic::Runner::default().start(|mut runtime| async move {
        let mut h = Harness::new(&mut runtime, RoundRobin::default());
        let block = h.propose(&runtime, 6).await;
        h.reporter
            .report(h.nullification(block.inner().context.round));
        h.reporter.prepared(&block);
        assert!(!h
            .reporter
            .prepare(&block.inner().context, &mut ScriptedRng::new([])));
        h.reporter.report(h.certification(&block));
        let state = h.reporter.state.lock().unwrap();
        assert!(state.pending.is_none());
        assert_eq!(state.success_rates, vec![RATE_SCALE; 4]);
    });
}

#[test]
fn wait_returns_before_proposal_pacing() {
    let config = deterministic::Config::default().with_rng(ScriptedRng::new([u64::MAX]));
    deterministic::Runner::new(config).start(|runtime| async move {
        let mut h = Harness::new(&mut test_rng(), RoundRobin::default());
        h.fail(&runtime, 6).await;
        h.application = Application::new(500, 0).with_handoffs(h.reporter.clone());
        let (context, parent) = h.inputs(10);
        let result = h
            .application
            .prepare(
                (runtime.child("wait"), context),
                ancestry::from_iter([parent]),
                (),
            )
            .now_or_never();
        assert!(matches!(result, Some(Handoff::Wait)));
    });
}
