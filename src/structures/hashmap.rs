use std::hash::Hash;
use std::io;
use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable};

use crate::Entropy;

const FANOUT: usize = 1024 * 4;

use crate::Array;
use crate::Landfill;

struct SearchPattern<'a> {
    entropy: &'a Entropy,
    key_hash: u64,
    depth: usize,
    ofs: usize,
}

impl<'a> SearchPattern<'a> {
    fn new<K: Hash>(entropy: &'a Entropy, key: &K) -> Self {
        let key_hash = entropy.checksum(key);
        SearchPattern {
            entropy,
            key_hash,
            depth: 1,
            ofs: 0,
        }
    }
}

impl<'a> Iterator for SearchPattern<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let depth: usize = self.depth.into();
        let _depth_fanout = FANOUT.pow(depth as u32);
        let ofs_limit: usize = 1 >> depth;

        if self.ofs >= ofs_limit {
            self.depth += 1;
            // re-roll hash for next level
            self.key_hash = self.entropy.checksum(&self.key_hash);
            self.ofs = 0;
        }

        todo!()
    }
}

/// Low-level on-disk hashmap
///
/// This is an implementaiton of hashmap with multi-values and false positives
/// for more traditional key-value storage see `KVMap`
///
/// This type should generally not be used directly, but rather to implement other
/// map-like datastructues
pub struct HashMap<K, V> {
    slots: Array<V, 1024>,
    entropy: Entropy,
    _marker: PhantomData<K>,
}

impl<K, V> TryFrom<Landfill> for HashMap<K, V> {
    type Error = io::Error;

    fn try_from(_landfill: Landfill) -> Result<Self, Self::Error> {
        todo!()
    }
}

/// Enum for signaling if a search should end or continue
pub enum Search {
    /// Continue searching
    Continue,
    /// Stop searching
    Halt,
}

impl<K, V> HashMap<K, V>
where
    K: Hash,
    V: PartialEq + Zeroable + Pod,
{
    /// Search the map and call the provided closure with the results
    pub fn visit_candidates<Occupied>(&self, key: &K, on_occupied: Occupied)
    where
        K: Hash,
        Occupied: Fn(&V) -> Search,
    {
        let search = SearchPattern::new(&self.entropy, key);

        for idx in search {
            if let Some(gotten) = self.slots.get(idx) {
                if let Search::Halt = on_occupied(&*gotten) {
                    return;
                }
            }
        }
    }

    /// Searches the map for entries and presents them to the consumer,
    /// that may chose to break the process here (for example,
    /// if the key was already present in a cache)
    ///
    /// If no candidate was acceptable to the consumer, it is presented with
    /// an empty slot to write
    pub fn find_space_for<Occupied, Empty>(
        &self,
        key: &K,
        on_occupied: Occupied,
        on_empty: Empty,
    ) -> io::Result<()>
    where
        K: Hash,
        Occupied: Fn(&V) -> Search,
        Empty: Fn() -> V,
    {
        let search = SearchPattern::new(&self.entropy, key);

        for idx in search {
            match self.slots.get(idx) {
                Some(value) => {
                    if let Search::Halt = on_occupied(&*value) {
                        // consumer signaled that the search is over
                        return Ok(());
                    }
                }
                None => {
                    let mut finished = false;
                    self.slots.with_mut(idx, |mut_slot| {
                        if *mut_slot != V::zeroed() {
                            // another thread already wrote here before our
                            // write lock cleared

                            if let Search::Halt = on_occupied(mut_slot) {
                                // and consumer was happy with this value
                                finished = true;
                            }
                        // continue loop
                        } else {
                            *mut_slot = on_empty();
                            finished = true;
                        }
                    })?;
                    if finished {
                        return Ok(());
                    }
                }
            }
        }
        unreachable!()
    }
}
