use alto_types::{Activity, Block, Context, Scheme};
use commonware_actor::Feedback;
use commonware_consensus::{
    simplex::{
        elector::{Config, Elector, Mode},
        types::Proposal,
    },
    types::{Participant, Round, TermLength},
    Reporter,
};
use commonware_cryptography::{sha256::Digest, Digested, Digestible};
use commonware_utils::Probability;
use rand::Rng;
use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

/// Fixed-point denominator for the per-leader success estimates.
const RATE_SCALE: u64 = 1 << 16;

/// Per-leader success estimates for choosing whether to prepare early.
///
/// An early proposal becomes a sample once consensus casts our notarize vote for it. Each
/// sample's outcome contributes one eighth of the parent leader's new success estimate. That
/// estimate sets the chance of preparing early, and a 1/16 floor on that chance lets a leader
/// that recovers raise its estimate again. Ordinary proposals do not update the estimate because
/// they do not test early preparation.
#[derive(Clone)]
pub(crate) struct Handoffs<S: Scheme> {
    state: Arc<Mutex<State>>,
    _scheme: PhantomData<S>,
}

struct State {
    leader: Box<dyn Fn(Round) -> Option<Participant> + Send>,
    term_length: TermLength,
    me: Option<Participant>,
    success_rates: Vec<u64>,
    last_outcome: Round,
    pending: Option<Sample>,
}

/// An early proposal whose outcome is still unknown.
struct Sample {
    proposal: Proposal<Digest>,
    /// Whether we have voted for `proposal`. Consensus can abandon a prepared proposal before
    /// voting, as when the build outlasts the leader timeout. The view's outcome then says
    /// nothing about the parent leader, so only a voted proposal counts as a sample.
    voted: bool,
}

impl<S: Scheme> Handoffs<S> {
    pub(crate) fn new<L: Config<S>>(scheme: &S, config: L) -> Self {
        let elector = config.build(scheme.participants());
        let term_length = elector.terms().length();
        Self {
            state: Arc::new(Mutex::new(State {
                leader: Box::new(move |round| {
                    let input = <<L::Elector as Elector<S>>::Mode as Mode>::early()?;
                    Some(elector.elect(round, input))
                }),
                term_length,
                me: scheme.me().map(|me| me.index()),
                success_rates: vec![RATE_SCALE; scheme.participants().len()],
                last_outcome: Round::zero(),
                pending: None,
            })),
            _scheme: PhantomData,
        }
    }

    /// Decides at random whether to prepare early, with the chance set by the parent leader's
    /// success estimate.
    pub(crate) fn prepare(&self, context: &Context, rng: &mut impl Rng) -> bool {
        let state = self.state.lock().unwrap();
        let parent = Round::new(context.round.epoch(), context.parent.0);
        let Some(leader) = (state.leader)(parent) else {
            return false;
        };
        let rate = state.success_rates[usize::from(leader)].max(RATE_SCALE / 16);
        Probability::new(rate, RATE_SCALE).unwrap().sample(rng)
    }

    /// Retains the first unresolved early proposal.
    pub(crate) fn prepared(&self, block: &Digested<Block>) {
        let context = &block.inner().context;
        let mut state = self.state.lock().unwrap();

        // A new preparation can finish before the previous outcome is reported. Keep the
        // pending sample so that pipelining cannot discard that outcome.
        if state.pending.is_some() || context.round <= state.last_outcome {
            return;
        }
        state.pending = Some(Sample {
            proposal: Proposal::new(context.round, context.parent.0, block.digest()),
            voted: false,
        });
    }
}

impl<S: Scheme> Reporter for Handoffs<S> {
    type Activity = Activity<S>;

    fn report(&mut self, activity: Self::Activity) -> Feedback {
        let mut state = self.state.lock().unwrap();
        let (round, proposal, finalized) = match activity {
            Activity::Notarize(vote) => {
                let me = state.me;
                if let Some(sample) = &mut state.pending {
                    if me == Some(vote.attestation.signer) && vote.proposal == sample.proposal {
                        sample.voted = true;
                    }
                }
                return Feedback::Ok;
            }
            Activity::Certification(n) => (n.proposal.round, Some(n.proposal), false),
            Activity::Finalization(f) => (f.proposal.round, Some(f.proposal), true),
            Activity::Nullification(n) => (n.round, None, false),
            _ => return Feedback::Ok,
        };

        // Finalization cancels certification work through its round. Otherwise, close older
        // samples only on our term-start outcomes. Other rounds' results can arrive before
        // a pending certification.
        if round <= state.last_outcome {
            return Feedback::Ok;
        }
        let own_term_start = round.view().is_term_start(state.term_length)
            && state.me.is_some_and(|me| (state.leader)(round) == Some(me));
        if !finalized && !own_term_start {
            return Feedback::Ok;
        }

        // Only a voted sample has an outcome. At the sampled round, a certificate credits the
        // parent leader and a nullification debits it. Our single vote at that view means any
        // certificate there names the sampled proposal.
        //
        // A later finalization can settle the sample first. An honest leader signs one proposal per
        // view, so a finalized block whose parent lies in the sampled view proves our proposal
        // certified. Simplex requires every view between a block and its parent to be nullified, so
        // a finalized block whose parent precedes the sampled view proves that view was nullified.
        // A parent after the sampled view reveals nothing.
        let update = state
            .pending
            .as_ref()
            .filter(|sample| sample.voted)
            .and_then(|sample| {
                let sampled = sample.proposal.round;
                let success = if round == sampled {
                    proposal.is_some()
                } else if finalized && round > sampled {
                    let parent = proposal.as_ref()?.parent;
                    if parent > sampled.view() {
                        return None;
                    }
                    parent == sampled.view()
                } else {
                    return None;
                };
                let parent = Round::new(sampled.epoch(), sample.proposal.parent);
                Some(((state.leader)(parent)?, success))
            });
        if let Some((leader, success)) = update {
            let rate = &mut state.success_rates[usize::from(leader)];
            *rate = if success {
                *rate + (RATE_SCALE - *rate).div_ceil(8)
            } else {
                *rate * 7 / 8
            };
        }
        state.last_outcome = round;
        if state
            .pending
            .as_ref()
            .is_some_and(|sample| sample.proposal.round <= round)
        {
            state.pending = None;
        }
        Feedback::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::Application;
    use alto_types::{StandardScheme, EPOCH, NAMESPACE};
    use bytes::Bytes;
    use commonware_consensus::{
        marshal::ancestry,
        simplex::{
            elector::RoundRobin,
            scheme::bls12381_threshold::standard,
            types::{Finalization, Finalize, Notarization, Notarize, Nullification, Nullify},
        },
        types::{Height, View, ViewDelta},
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

    /// Exercises validator 2 in a four-validator round-robin schedule.
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

        async fn propose(
            &mut self,
            runtime: &deterministic::Context,
            view: u64,
        ) -> Digested<Block> {
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

        /// Records an early proposal that consensus has not voted for.
        async fn candidate(
            &mut self,
            runtime: &deterministic::Context,
            view: u64,
        ) -> Digested<Block> {
            let block = self.propose(runtime, view).await;
            self.reporter.prepared(&block);
            block
        }

        /// Records an early proposal and our vote for it, so outcome tests can control which
        /// attempts are sampled.
        async fn sample(&mut self, runtime: &deterministic::Context, view: u64) -> Digested<Block> {
            let block = self.candidate(runtime, view).await;
            self.reporter.report(self.vote(&block));
            block
        }

        fn proposal(block: &Digested<Block>) -> Proposal<Digest> {
            let context = &block.inner().context;
            Proposal::new(context.round, context.parent.0, block.digest())
        }

        /// Validator 2's own notarize vote for `block`.
        fn vote(&self, block: &Digested<Block>) -> Activity<StandardScheme> {
            Activity::Notarize(Notarize::sign(&self.schemes[2], Self::proposal(block)).unwrap())
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
            let proposal = Self::proposal(block);
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
            let proposal = Self::proposal(block);
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

            // Repeated failures on the same parent leader must each lower its success estimate.
            let mut previous = RATE_SCALE;
            for view in [6, 10, 14, 18] {
                h.fail(&runtime, view).await;
                let rate = h.reporter.state.lock().unwrap().success_rates[1];
                assert!(rate < previous);
                previous = rate;
            }

            // Each early success recovers only part of the loss, including the first one.
            for view in [22, 26, 30, 34] {
                let block = h.sample(&runtime, view).await;
                h.reporter.report(h.certification(&block));
                let rate = h.reporter.state.lock().unwrap().success_rates[1];
                assert!(rate > previous);
                assert!(rate - previous <= RATE_SCALE / 8);
                assert!(rate < RATE_SCALE);
                previous = rate;
            }

            // Rounding must let a nearly recovered estimate reach full confidence.
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

            // First fail every early proposal while certifying ordinary ones. Then let early
            // proposals succeed, keeping the same history across both phases.
            for recover in [false, true] {
                let mut late_early_votes = 0;
                let mut first_recovery = true;
                for step in 0..256 {
                    let view = 6 + 4 * (step + u64::from(recover) * 256);
                    let before = h.reporter.state.lock().unwrap().success_rates[1];
                    match h.prepare(&runtime, view).await {
                        Handoff::Vote(block) => {
                            h.reporter.report(h.vote(&block));

                            // Judge the latter half of each phase, after the estimate can adapt.
                            if step >= 128 {
                                late_early_votes += 1;
                            }

                            if recover {
                                // The first successful probe must retain enough failure history
                                // to favor waiting.
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
                            // Success after waiting gives no evidence that early preparation works.
                            let block = h.propose(&runtime, view).await;
                            h.reporter.report(h.certification(&block));
                            assert_eq!(h.reporter.state.lock().unwrap().success_rates[1], before);
                        }
                        Handoff::Stage(_) => panic!("handoffs must either vote early or wait"),
                    }
                }

                if recover {
                    // Sustained early successes must restore frequent early voting.
                    assert!(
                        late_early_votes > 100,
                        "successful probes must restore early voting"
                    );
                    assert!(
                        h.reporter.state.lock().unwrap().success_rates[1] > RATE_SCALE * 99 / 100
                    );
                } else {
                    // Failures must make early votes rare while preserving probes for recovery.
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

        // Full confidence must select early preparation without consuming randomness.
        assert!(h.reporter.prepare(&context, &mut ScriptedRng::new([])));

        // The 64-bit draw cutoff is 1/16 of its range at zero confidence and 1/2 at half
        // confidence. Only draws below the cutoff may prepare early.
        for (rate, threshold) in [(0, 1_u64 << 60), (RATE_SCALE / 2, 1_u64 << 63)] {
            h.reporter.state.lock().unwrap().success_rates[1] = rate;
            assert!(h
                .reporter
                .prepare(&context, &mut ScriptedRng::new([threshold - 1])));
            assert!(!h
                .reporter
                .prepare(&context, &mut ScriptedRng::new([threshold])));

            // A preparation decision cannot create a sample before a block is built.
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
            h.reporter.report(h.vote(&block));
            let context = &block.inner().context;

            // Nullifying the parent does not establish the outcome of our early proposal.
            h.reporter
                .report(h.nullification(Round::new(EPOCH, context.parent.0)));
            assert_eq!(h.reporter.state.lock().unwrap().success_rates, previous);

            // Our own nullification lowers only the estimate for the leader of our parent.
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

            // Lower confidence so a credited certification produces an observable increase.
            h.fail(&runtime, 6).await;
            let before = h.reporter.state.lock().unwrap().success_rates.clone();
            let block = h.sample(&runtime, 10).await;
            let round = block.inner().context.round;

            // Individual votes, notarizations before application certification, and other
            // leaders' rounds must leave this sample unresolved.
            h.reporter.report(Activity::Nullify(
                Nullify::sign::<Digest>(&h.schemes[0], round).unwrap(),
            ));
            let Activity::Certification(notarization) = h.certification(&block) else {
                unreachable!()
            };
            h.reporter.report(Activity::Notarization(notarization));
            h.reporter
                .report(h.nullification(Round::new(EPOCH, View::new(11))));
            assert_eq!(h.reporter.state.lock().unwrap().success_rates, before);

            // Certification credits the sample once. Repeats and a later nullification must
            // leave that credited outcome unchanged.
            h.reporter.report(h.certification(&block));
            let after = h.reporter.state.lock().unwrap().success_rates.clone();
            assert!(after[1] > before[1]);
            h.reporter.report(h.certification(&block));
            h.reporter.report(h.nullification(round));
            assert_eq!(h.reporter.state.lock().unwrap().success_rates, after);

            // A nullified sample cannot earn credit from a late certificate. Replaying an
            // older certificate must also leave the estimate unchanged.
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

            // A prior loss makes accidental success credit for the replacement observable.
            h.fail(&runtime, 6).await;
            let before = h.reporter.state.lock().unwrap().success_rates.clone();
            let block = h.candidate(&runtime, 10).await;

            // Consensus dropped the early request before voting, and the same view certifies an
            // ordinary proposal built on an older parent.
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

            // Resolving the view retires the candidate without crediting the replacement.
            // A later certificate for the candidate must not reopen it.
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

            // Leave the first sample unresolved while preparation finishes for a later view.
            h.fail(&runtime, 6).await;
            let before = h.reporter.state.lock().unwrap().success_rates[1];
            let block = h.sample(&runtime, 10).await;
            let newer = h.sample(&runtime, 14).await;

            // The overlapping preparation must not prevent the first sample from earning credit.
            h.reporter.report(h.certification(&block));
            let after = h.reporter.state.lock().unwrap().success_rates[1];
            assert!(after > before);

            // The newer proposal was not sampled, so its nullification cannot affect the estimate.
            h.reporter
                .report(h.nullification(newer.inner().context.round));
            assert_eq!(h.reporter.state.lock().unwrap().success_rates[1], after);

            // Once the pending sample resolves, a fresh sample can affect the estimate again.
            h.fail(&runtime, 18).await;
            assert!(h.reporter.state.lock().unwrap().success_rates[1] < after);
        });
    }

    #[test]
    fn finalization_can_resolve_an_early_sample_without_local_certification() {
        deterministic::Runner::default().start(|mut runtime| async move {
            let mut h = Harness::new(&mut runtime, RoundRobin::default());

            // Finalization can arrive without local application certification. It must still
            // credit the exact early proposal that was sampled.
            h.fail(&runtime, 6).await;
            let before = h.reporter.state.lock().unwrap().success_rates[1];
            let block = h.sample(&runtime, 10).await;
            h.reporter.report(h.finalization(&block));
            assert!(h.reporter.state.lock().unwrap().success_rates[1] > before);
        });
    }

    #[test]
    fn a_finalized_child_credits_an_unresolved_sample() {
        deterministic::Runner::default().start(|mut runtime| async move {
            // Cover finalizing a later leader's view in rotating mode and a later view of our
            // own term in stable mode. An earlier failed handoff lowers the estimate so that a
            // credit is observable.
            for (config, prior, sampled, finalized, next) in [
                (RoundRobin::default(), 2, 6, 7, 10),
                (
                    RoundRobin::default().with_term(
                        TermLength::new(NZU32!(4)),
                        Duration::from_secs(12),
                        ViewDelta::new(4),
                    ),
                    5,
                    21,
                    22,
                    37,
                ),
            ] {
                let mut h = Harness::new(&mut runtime, config);
                h.fail(&runtime, prior).await;
                let before = h.reporter.state.lock().unwrap().success_rates[1];

                // A child of the sampled block finalizes before the sample's own outcome arrives.
                // That child proves the sampled proposal certified, so the sample earns credit.
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
                let credited = h.reporter.state.lock().unwrap().success_rates[1];
                assert!(credited > before);

                // Late preparation must not reopen a view that finality has already retired.
                h.reporter.prepared(&stale);
                assert!(h.reporter.state.lock().unwrap().pending.is_none());

                // Retiring the sample must free the slot for the next handoff's outcome.
                h.fail(&runtime, next).await;
                assert_eq!(
                    h.reporter.state.lock().unwrap().success_rates[1],
                    credited * 7 / 8,
                    "finalization of view {finalized} must retire the sample from view {sampled}"
                );
            }
        });
    }

    #[test]
    fn a_finalization_beyond_a_sampled_view_resolves_it_by_ancestry() {
        deterministic::Runner::default().start(|mut runtime| async move {
            // A finalized block whose parent lies below the sampled view proves the view was
            // nullified. One whose parent lies above it reveals nothing and retires the sample
            // without an outcome.
            for (finalized, parent_view, nullified) in [(7, 5, true), (8, 7, false)] {
                let mut h = Harness::new(&mut runtime, RoundRobin::default());
                h.fail(&runtime, 2).await;
                let before = h.reporter.state.lock().unwrap().success_rates[1];
                h.sample(&runtime, 6).await;

                // `inputs(view)` returns a parent from `view - 1`, so this block sits at
                // `parent_view`.
                let (_, parent) = h.inputs(parent_view + 1);
                let (mut context, _) = h.inputs(finalized);
                context.parent = (parent.inner().context.round.view(), parent.digest());
                let block = h
                    .application
                    .propose(
                        (runtime.child("finalized"), context),
                        ancestry::from_iter([parent]),
                        (),
                    )
                    .await
                    .unwrap();
                h.reporter.report(h.finalization(&block));
                let state = h.reporter.state.lock().unwrap();
                let expected = if nullified { before * 7 / 8 } else { before };
                assert_eq!(state.success_rates[1], expected);
                assert!(state.pending.is_none());
            }
        });
    }

    #[test]
    fn an_unvoted_candidate_does_not_update_the_estimate() {
        deterministic::Runner::default().start(|mut runtime| async move {
            let mut h = Harness::new(&mut runtime, RoundRobin::default());

            // Consensus can abandon a prepared proposal before voting, as when the build outlasts
            // the leader timeout. Another validator's vote for that proposal must not make it a
            // sample, and nullifying its view must not charge the parent leader.
            let block = h.candidate(&runtime, 6).await;
            h.reporter.report(Activity::Notarize(
                Notarize::sign(&h.schemes[0], Harness::proposal(&block)).unwrap(),
            ));
            h.reporter
                .report(h.nullification(block.inner().context.round));
            {
                let state = h.reporter.state.lock().unwrap();
                assert_eq!(state.success_rates, vec![RATE_SCALE; 4]);
                assert!(state.pending.is_none());
            }

            // Our vote for an ordinary rebuild in the same view must not make the abandoned
            // candidate a sample either.
            let block = h.candidate(&runtime, 10).await;
            let (mut context, _) = h.inputs(10);
            let (_, parent) = h.inputs(9);
            context.parent = (parent.inner().context.round.view(), parent.digest());
            let rebuilt = h
                .application
                .propose(
                    (runtime.child("rebuilt"), context),
                    ancestry::from_iter([parent]),
                    (),
                )
                .await
                .unwrap();
            h.reporter.report(h.vote(&rebuilt));
            h.reporter
                .report(h.nullification(block.inner().context.round));
            {
                let state = h.reporter.state.lock().unwrap();
                assert_eq!(state.success_rates, vec![RATE_SCALE; 4]);
                assert!(state.pending.is_none());
            }

            // The retired candidate frees the slot for a voted sample.
            h.fail(&runtime, 14).await;
            assert_eq!(
                h.reporter.state.lock().unwrap().success_rates[1],
                RATE_SCALE * 7 / 8
            );
        });
    }

    #[test]
    fn finalizing_the_parent_preserves_its_childs_pending_sample() {
        deterministic::Runner::default().start(|mut runtime| async move {
            let mut h = Harness::new(&mut runtime, RoundRobin::default());

            // Finalizing the parent leaves the child's sample pending. A later certificate
            // for the child must still increase the estimate.
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
            // Only the first view of our four-view term is an early handoff opportunity.
            let config = RoundRobin::default().with_term(
                TermLength::new(NZU32!(4)),
                Duration::from_secs(12),
                ViewDelta::new(4),
            );
            let mut h = Harness::new(&mut runtime, config);
            let block = h.sample(&runtime, 21).await;

            // An outcome after the first view must leave the term-start sample unresolved.
            h.reporter
                .report(h.nullification(Round::new(EPOCH, View::new(22))));
            assert_eq!(
                h.reporter.state.lock().unwrap().success_rates[1],
                RATE_SCALE
            );

            // The term-start outcome must still lower the estimate and allow waiting at the
            // next handoff to this validator.
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

            // Without ancestry there is no proposal to sample, even if its view is nullified.
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

            // Keep the next build asleep on its pacing delay, then cancel it by dropping
            // the pending future before any block can be returned.
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

            // Neither path completed an early proposal, so neither nullification may lower
            // a parent's estimate or leave a pending sample.
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

            // Consensus can resolve the view before the application's preparation result arrives.
            let block = h.propose(&runtime, 6).await;
            h.reporter
                .report(h.nullification(block.inner().context.round));

            // A late result and its certificate must not create or credit a sample for that view.
            h.reporter.prepared(&block);
            h.reporter.report(h.certification(&block));
            let state = h.reporter.state.lock().unwrap();
            assert!(state.pending.is_none());
            assert_eq!(state.success_rates, vec![RATE_SCALE; 4]);
        });
    }

    #[test]
    fn wait_returns_before_proposal_pacing() {
        // Force a wait decision after a failure while the next proposal has a pacing delay.
        let config = deterministic::Config::default().with_rng(ScriptedRng::new([u64::MAX]));
        deterministic::Runner::new(config).start(|runtime| async move {
            let mut h = Harness::new(&mut test_rng(), RoundRobin::default());
            h.fail(&runtime, 6).await;
            h.application = Application::new(500, 0).with_handoffs(h.reporter.clone());
            let (context, parent) = h.inputs(10);

            // Returning Wait must finish on the first poll, before entering proposal pacing.
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
}
