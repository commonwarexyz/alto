use alto_types::{leader_index, Evaluation, Identity, Signature};
use commonware_codec::Encode;
use commonware_consensus::{
    threshold_simplex::types::View, Supervisor as Su, ThresholdSupervisor as TSu,
};
use commonware_cryptography::{
    bls12381::{
        dkg::ops::evaluate_all,
        primitives::{
            group,
            poly::{self, Poly},
            variant::MinSig,
        },
    },
    ed25519,
};
use commonware_resolver::p2p;
use std::collections::HashMap;

/// Implementation of [commonware_consensus::Supervisor].
#[derive(Clone)]
pub struct Supervisor {
    identity: Identity,
    polynomial: Vec<Evaluation>,
    participants: Vec<ed25519::PublicKey>,
    participants_map: HashMap<ed25519::PublicKey, u32>,

    share: group::Share,
}

impl Supervisor {
    /// Create a new [Supervisor].
    pub fn new(
        polynomial: Poly<Evaluation>,
        mut participants: Vec<ed25519::PublicKey>,
        share: group::Share,
    ) -> Self {
        // Setup participants
        participants.sort();
        let mut participants_map = HashMap::new();
        for (index, validator) in participants.iter().enumerate() {
            participants_map.insert(validator.clone(), index as u32);
        }
        let identity = *poly::public::<MinSig>(&polynomial);
        let polynomial = evaluate_all::<MinSig>(&polynomial, participants.len() as u32);

        // Return supervisor
        Self {
            identity,
            polynomial,
            participants,
            participants_map,
            share,
        }
    }
}

impl p2p::Coordinator for Supervisor {
    type PublicKey = ed25519::PublicKey;

    fn peers(&self) -> &Vec<Self::PublicKey> {
        &self.participants
    }

    fn peer_set_id(&self) -> u64 {
        0
    }
}

impl Su for Supervisor {
    type Index = View;
    type PublicKey = ed25519::PublicKey;

    fn leader(&self, _: Self::Index) -> Option<Self::PublicKey> {
        unimplemented!("only defined in supertrait")
    }

    fn participants(&self, _: Self::Index) -> Option<&Vec<Self::PublicKey>> {
        Some(&self.participants)
    }

    fn is_participant(&self, _: Self::Index, candidate: &Self::PublicKey) -> Option<u32> {
        self.participants_map.get(candidate).cloned()
    }
}

impl TSu for Supervisor {
    type Seed = Signature;
    type Identity = Identity;
    type Polynomial = Vec<Evaluation>;
    type Share = group::Share;

    fn leader(&self, _: Self::Index, seed: Self::Seed) -> Option<Self::PublicKey> {
        let index = leader_index(seed.encode().as_ref(), self.participants.len());
        Some(self.participants[index].clone())
    }

    fn identity(&self) -> &Self::Identity {
        &self.identity
    }

    fn polynomial(&self, _: Self::Index) -> Option<&Self::Polynomial> {
        Some(&self.polynomial)
    }

    fn share(&self, _: Self::Index) -> Option<&Self::Share> {
        Some(&self.share)
    }
}
