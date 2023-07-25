use std::{
    hash::{Hash, Hasher},
    io,
};

use crate::Landfill;

use bytemuck_derive::*;
use rand::Rng;
use seahash::SeaHasher;

/// A once-initialized file carrying pseudorandom data
///
/// This can be used to have a persistant source of entropy, that will be
/// the same each time the database is opened, but differ between databases
///
/// Useful for DOS-resistant hashmaps etc
#[repr(C)]
#[derive(Clone, Copy, Zeroable, Pod)]
pub struct Entropy([u64; 4]);

/// A Tag that can be used to loosely identify this specific instantiation of
/// entropy.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Zeroable, Pod)]
pub struct Tag(u32);

impl TryFrom<&Landfill> for Entropy {
    type Error = io::Error;

    fn try_from(landfill: &Landfill) -> io::Result<Self> {
        landfill.get_static_or_init("entropy", || {
            let mut rng = rand::thread_rng();
            Entropy(rng.gen())
        })
    }
}

impl Entropy {
    /// Calculate a checksum of value `T` specific to this entropy set
    pub fn checksum<T: Hash>(&self, t: &T) -> u64 {
        let mut hasher =
            SeaHasher::with_seeds(self.0[0], self.0[1], self.0[2], self.0[3]);
        t.hash(&mut hasher);
        hasher.finish()
    }

    /// Generate a nonce, note this is not influenced in any way by the data,
    /// and is pseudorandom
    pub fn nonce(&self) -> u64 {
        rand::thread_rng().gen()
    }

    /// Return the tag loosely identifying this entropy set
    pub fn tag(&self) -> Tag {
        Tag(self.checksum(&()) as u32)
    }
}
