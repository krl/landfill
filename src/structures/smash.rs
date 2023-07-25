use std::hash::Hash;
use std::io;
use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable};

use crate::Entropy;

use crate::Array;
use crate::Landfill;

struct SearchPattern<'a> {
    entropy: &'a Entropy,
    key_hash: u64,
    fanout: usize,
    offset: usize,
    retries: usize,
    tries_limit: usize,
}

impl<'a> SearchPattern<'a> {
    fn new<K: Hash>(entropy: &'a Entropy, key: &K) -> Self {
        let key_hash = entropy.checksum(key);
        SearchPattern {
            entropy,
            key_hash,
            fanout: 1024,
            offset: 0,
            retries: 0,
            tries_limit: 1,
        }
    }
}

impl<'a> Iterator for SearchPattern<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        // We add together the following quantities to calculate the next
        // bucket index to use
        let slot =
			// the global offset
			self.offset +
			// the entropy state, modulo the currently active fanout
			(self.key_hash % self.fanout as u64) as usize +
		// how many sequential retries have been made
			self.retries;

        // FIXME: Calculating the next value eagerly is wasteful

        self.retries += 1;
        if self.retries == self.tries_limit {
            self.offset += self.fanout;
            self.fanout = self.fanout << 1;
            self.tries_limit = self.tries_limit << 1;
            self.key_hash = self.entropy.checksum(&self.key_hash);
            self.retries = 0;
        }

        Some(slot)
    }
}

/// Low-level on-disk hashmap
///
/// This is an implementaiton of hashmap with multi-values and false positives
/// for more traditional key-value storage see `KVMap`
///
/// This type should generally not be used directly, but rather to implement other
/// map-like datastructues
pub struct SmashMap<K, V> {
    slots: Array<V>,
    entropy: Entropy,
    _marker: PhantomData<K>,
}

impl<K, V> TryFrom<&Landfill> for SmashMap<K, V> {
    type Error = io::Error;

    fn try_from(landfill: &Landfill) -> Result<Self, Self::Error> {
        Ok(SmashMap {
            slots: Array::<V>::try_from(landfill)?,
            entropy: Entropy::try_from(landfill)?,
            _marker: PhantomData,
        })
    }
}

/// Enum for signaling if a search should end or continue
pub enum Search {
    /// Continue searching
    Continue,
    /// Stop searching
    Halt,
}

impl<K, V> SmashMap<K, V>
where
    K: Hash,
    V: PartialEq + Zeroable + Pod,
{
    /// Search the map and call the provided closure with the results
    pub fn get<Occupied>(&self, key: &K, mut on_occupied: Occupied)
    where
        K: Hash,
        Occupied: FnMut(&V) -> Search,
    {
        let search = SearchPattern::new(&self.entropy, key);

        for idx in search {
            if let Some(gotten) = self.slots.get(idx) {
                if let Search::Halt = on_occupied(&*gotten) {
                    return;
                }
            } else {
                return;
            }
        }
    }

    /// Searches the map for entries and presents them to the consumer,
    /// that may chose to break the process here (for example,
    /// if the key was already present in a cache)
    ///
    /// If no candidate was acceptable to the consumer, it is presented with
    /// an empty slot to write
    pub fn insert<Occupied, Empty>(
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
