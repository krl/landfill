use std::hash::{Hash, Hasher};

use crate::header::StaticHeader;
use seahash::SeaHasher;

#[derive(Clone)]
pub(crate) struct CheckSummer {
    key_a: u64,
    key_b: u64,
    key_c: u64,
    key_d: u64,
}

impl CheckSummer {
    pub fn new(key_a: u64, key_b: u64, key_c: u64, key_d: u64) -> Self {
        CheckSummer {
            key_a,
            key_b,
            key_c,
            key_d,
        }
    }

    pub fn new_from_header(header: &StaticHeader) -> Self {
        CheckSummer {
            key_a: header.key_a,
            key_b: header.key_b,
            key_c: header.key_c,
            key_d: header.key_d,
        }
    }

    pub fn checksum<T: Hash>(&self, t: T) -> u64 {
        let mut hasher = SeaHasher::with_seeds(self.key_a, self.key_b, self.key_c, self.key_d);
        t.hash(&mut hasher);
        hasher.finish()
    }

    pub fn checksum_truncated<T: Hash>(&self, t: T) -> u32 {
        self.checksum(t) as u32
    }
}
