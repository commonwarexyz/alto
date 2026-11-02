use alto_types::{Activity, Block, Context, Scheme, EPOCH};
use commonware_actor::Feedback;
use commonware_consensus::{
    simplex::{
        elector::{Config, Elector, Mode},
        types::Proposal,
    },
    types::{Participant, Round, TermLength, View},
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
/// Each sampled outcome contributes one eighth of the parent leader's new success estimate.
/// That estimate sets the chance of preparing early, with a 1/16 minimum so a recovered
/// leader's estimate can rise again. Ordinary proposals do not update the estimate because
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
    pending: Option<Proposal<Digest>>,
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

    pub(crate) fn prepare(&self, context: &Context, rng: &mut impl Rng) -> bool {
        let state = self.state.lock().unwrap();
        if context.round.epoch() != EPOCH
            || context.round <= state.last_outcome
            || !context.round.view().is_term_start(state.term_length)
            || context.parent.0 == View::zero()
        {
            return false;
        }
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
        state.pending = Some(Proposal::new(
            context.round,
            context.parent.0,
            block.digest(),
        ));
    }
}

impl<S: Scheme> Reporter for Handoffs<S> {
    type Activity = Activity<S>;

    fn report(&mut self, activity: Self::Activity) -> Feedback {
        let (round, proposal, finalized) = match activity {
            Activity::Certification(n) => (n.proposal.round, Some(n.proposal), false),
            Activity::Finalization(f) => (f.proposal.round, Some(f.proposal), true),
            Activity::Nullification(n) => (n.round, None, false),
            _ => return Feedback::Ok,
        };

        // Finalization cancels certification work through its round. Otherwise, close older
        // samples only on our term-start outcomes. Other rounds' results can arrive before
        // a pending certification.
        let mut state = self.state.lock().unwrap();
        if round.epoch() != EPOCH || round <= state.last_outcome {
            return Feedback::Ok;
        }
        let own_term_start = round.view().is_term_start(state.term_length)
            && state.me.is_some_and(|me| (state.leader)(round) == Some(me));
        if !finalized && !own_term_start {
            return Feedback::Ok;
        }
        let leader = state
            .pending
            .as_ref()
            .filter(|sample| {
                sample.round == round && proposal.as_ref().is_none_or(|p| p == *sample)
            })
            .and_then(|sample| (state.leader)(Round::new(sample.round.epoch(), sample.parent)));
        if let Some(leader) = leader {
            let rate = &mut state.success_rates[usize::from(leader)];
            *rate = if proposal.is_some() {
                *rate + (RATE_SCALE - *rate).div_ceil(8)
            } else {
                *rate * 7 / 8
            };
        }
        state.last_outcome = round;
        if state
            .pending
            .as_ref()
            .is_some_and(|sample| sample.round <= round)
        {
            state.pending = None;
        }
        Feedback::Ok
    }
}

#[cfg(test)]
mod tests;
