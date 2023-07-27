use std::hash::Hash;
use std::io;
use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable};

use crate::helpers;
use crate::Array;
use crate::Entropy;
use crate::Landfill;

const INITIAL_FANOUT: usize = 1024;

/// Low-level on-disk hashmap
///
/// This is an implementaiton of hashmap with multi-values and false positives
///
/// This type should generally not be used directly, but rather be used as a base
/// to implement other map-like datastructues
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
pub enum SearchNext {
    /// Proceed with searching
    Proceed,
    /// Stop searching
    Halt,
}

pub struct SearchPattern<'a> {
    entropy_source: &'a Entropy,
    entropy_state: u64,
    fanout: usize,
    offset: usize,
    retries: usize,
    tries_limit: usize,
}

impl<'a> SearchPattern<'a> {
    pub fn proceed(&self) -> SearchNext {
        SearchNext::Proceed
    }

    pub fn halt(&self) -> SearchNext {
        SearchNext::Halt
    }

    pub fn tag_u8(&self) -> u8 {
        let slice = &[self.entropy_state];
        let bytes: &[u8] = bytemuck::cast_slice(slice);
        bytes[0]
    }

    pub fn tag_u16(&self) -> u16 {
        let slice = &[self.entropy_state];
        let bytes: &[u16] = bytemuck::cast_slice(slice);
        bytes[0]
    }

    pub fn tag_u32(&self) -> u32 {
        let slice = &[self.entropy_state];
        let bytes: &[u32] = bytemuck::cast_slice(slice);
        bytes[0]
    }

    pub fn tag_u64(&self) -> u64 {
        self.entropy_state
    }

    fn new<K: Hash>(key: &K, entropy_source: &'a Entropy) -> Self {
        let entropy_state = entropy_source.checksum(key);
        SearchPattern {
            entropy_source,
            entropy_state,
            fanout: INITIAL_FANOUT,
            offset: 0,
            retries: 0,
            tries_limit: 1,
        }
    }

    fn get_slot(&self) -> usize {
        // the global offset
        self.offset
		// the entropy state, modulo the currently active fanout
            + (self.entropy_state % self.fanout as u64) as usize
		// how many sequential retries have been made
            + self.retries
    }

    fn calculate_next(&mut self) {
        self.retries += 1;
        if self.retries == self.tries_limit {
            self.offset += self.fanout;
            self.fanout = self.fanout << 1;
            self.tries_limit = self.tries_limit << 1;
            self.entropy_state =
                self.entropy_source.checksum(&self.entropy_state);
            self.retries = 0;
        }
    }
}

impl<K, V> SmashMap<K, V>
where
    K: Hash,
    V: Zeroable + Pod,
{
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
        mut on_empty: Empty,
    ) -> io::Result<()>
    where
        Occupied: Fn(&SearchPattern, &V) -> SearchNext,
        Empty: FnMut(&SearchPattern) -> io::Result<V>,
    {
        let mut search = SearchPattern::new(key, &self.entropy);
        loop {
            let slot = search.get_slot();

            match self.slots.get(slot) {
                Some(value) => {
                    if let SearchNext::Halt = on_occupied(&search, &*value) {
                        // consumer signaled that the search is over
                        return Ok(());
                    }
                }
                None => {
                    // Encountered an empty slot
                    let mut finished = false;

                    self.slots.with_mut(slot, |mut_slot| {
                        if !helpers::is_all_zeroes(&[*mut_slot]) {
                            // another thread already wrote here before our
                            // write lock cleared
                            if let SearchNext::Halt =
                                on_occupied(&search, mut_slot)
                            {
                                // and consumer was happy with this value
                                finished = true;
                            }
                        } else {
                            *mut_slot = on_empty(&search)?;
                            finished = true;
                        }
                        Ok::<_, io::Error>(())
                    })??;
                    if finished {
                        return Ok(());
                    }
                }
            }
            search.calculate_next()
        }
    }

    /// Search the map and call the provided closure with the results
    pub fn get<Occupied>(&self, key: &K, mut on_occupied: Occupied)
    where
        K: Hash,
        Occupied: FnMut(&SearchPattern, &V) -> SearchNext,
    {
        let mut search = SearchPattern::new(key, &self.entropy);
        loop {
            let slot = search.get_slot();

            match self.slots.get(slot) {
                Some(value) => {
                    if let SearchNext::Halt = on_occupied(&search, &*value) {
                        return;
                    }
                }
                None => {
                    return;
                }
            }
            search.calculate_next()
        }
    }
}
