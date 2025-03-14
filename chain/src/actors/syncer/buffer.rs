use alto_types::Block;
use commonware_cryptography::{ed25519::PublicKey, sha256::Digest};
use std::collections::{HashMap, VecDeque};

pub struct Buffer {
    cache: usize,
    queues: HashMap<PublicKey, VecDeque<Digest>>,
    stored: HashMap<Digest, (usize, Block)>,
}

impl Buffer {
    pub fn new(cache: usize) -> Self {
        Self {
            cache,
            queues: HashMap::new(),
            stored: HashMap::new(),
        }
    }

    pub fn add(&mut self, sender: PublicKey, block: Block) {
        // Get the entry for the author
        let entry = self.queues.entry(sender).or_default();

        // If the cache is full, remove the oldest item
        if entry.len() >= self.cache {
            let oldest = entry.pop_front().unwrap();
            let stored_entry = self.stored.get_mut(&oldest).unwrap();
            stored_entry.0 -= 1;
            if stored_entry.0 == 0 {
                self.stored.remove(&oldest);
            }
        }

        // Add the block to the cache
        let digest = block.digest();
        entry.push_back(digest);
        let stored_entry = self.stored.entry(digest).or_insert((1, block));
        stored_entry.0 += 1;
    }

    pub fn get(&self, digest: &Digest) -> Option<&Block> {
        self.stored.get(digest).map(|(_, block)| block)
    }
}
