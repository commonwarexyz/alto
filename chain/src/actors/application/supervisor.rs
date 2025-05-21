use alto_types::{leader_index, Identity, Signature};
use commonware_consensus::{
    threshold_simplex::types::{Seed, View},
    Supervisor as Su, ThresholdSupervisor as TSu,
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
    ed25519::PublicKey,
};
use std::collections::HashMap;

/// Implementation of `commonware-consensus::Supervisor`.
#[derive(Clone)]
pub struct Supervisor {
    identity: Identity,
    polynomial: Vec<Identity>,
    participants: Vec<PublicKey>,
    participants_map: HashMap<PublicKey, u32>,

    share: group::Share,
}

impl Supervisor {
    pub fn new(
        polynomial: Poly<Identity>,
        mut participants: Vec<PublicKey>,
        share: group::Share,
    ) -> Self {
        // Setup participants
        participants.sort();
        let mut participants_map = HashMap::new();
        for (index, validator) in participants.iter().enumerate() {
            participants_map.insert(validator.clone(), index as u32);
        }
        let identity = *poly::public::<MinSig>(&polynomial);
        let polynomial = evaluate_all::<MinSig>(&polynomial);

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

impl Su for Supervisor {
    type Index = View;
    type PublicKey = PublicKey;

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
    type Polynomial = Vec<Identity>;
    type Share = group::Share;

    fn leader(&self, view: Self::Index, seed: Self::Seed) -> Option<Self::PublicKey> {
        let seed = Seed::new(view, seed);
        let index = leader_index(&seed, self.participants.len());
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
