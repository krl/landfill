use std::borrow::Borrow;
use std::hash::Hash;
use std::io;
use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable};
use bytemuck_derive::*;

use crate::{AppendOnly, Landfill, SmashMap};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Entry {
    k_ofs: u64,
    v_ofs_relative: u32,
    tag: u32,
}

/// A map structure where each key can be set only once
///
/// This allows the get function to safely return unwrapped references
/// to the values, while still allowing concurrent inserts.
pub struct OnceMap<K, V> {
    data: AppendOnly,
    index: SmashMap<K, Entry>,
    _marker: PhantomData<V>,
}

impl<K, V> TryFrom<&Landfill> for OnceMap<K, V> {
    type Error = io::Error;

    fn try_from(landfill: &Landfill) -> Result<Self, Self::Error> {
        let landfill = landfill.branch("oncemap");

        let data = AppendOnly::try_from(&landfill)?;
        let index = SmashMap::try_from(&landfill)?;

        Ok(OnceMap {
            data,
            index,
            _marker: PhantomData,
        })
    }
}

impl<K, V> OnceMap<K, V>
where
    K: Hash + Zeroable + Pod + PartialEq + Eq,
    V: Zeroable + Pod,
{
    /// Insert a key-value pair into the map
    pub fn insert(&self, k: K, v: V) -> io::Result<()> {
        self.index.insert(
            &k,
            |search, entry| {
                let search_tag = search.tag_u32();

                if search_tag == entry.tag {
                    let stored_key = self.data.get::<K>(entry.k_ofs);

                    if k == *stored_key {
                        // we already have this key set
                        search.halt()
                    } else {
                        search.proceed()
                    }
                } else {
                    search.proceed()
                }
            },
            |search| {
                let k_ofs = self.data.insert(k)?;
                let v_ofs_relative = (self.data.insert(v)? - k_ofs) as u32;

                Ok(Entry {
                    k_ofs,
                    v_ofs_relative,
                    tag: search.tag_u32(),
                })
            },
        )
    }

    /// Gets the value corresponding to the key, if any
    pub fn get<O: Borrow<K>>(&self, o: &O) -> Option<&V> {
        let mut result = None;
        let k = o.borrow();
        self.index.get(k, |search, entry| {
            let search_tag = search.tag_u32();

            if search_tag == entry.tag {
                let stored_key = self.data.get::<K>(entry.k_ofs);

                if stored_key == k {
                    // found it!
                    let v_ofs = entry.k_ofs + entry.v_ofs_relative as u64;
                    result = Some(self.data.get::<V>(v_ofs));
                    search.halt()
                } else {
                    search.proceed()
                }
            } else {
                search.proceed()
            }
        });
        result
    }
}
