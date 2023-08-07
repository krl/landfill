use std::borrow::Borrow;
use std::hash::Hash;
use std::marker::PhantomData;
use std::{io, mem};

use bytemuck::{Pod, Zeroable};
use bytemuck_derive::*;

use crate::{AppendOnly, GuardedLandfill, SmashMap, Substructure};

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

impl<K, V> Substructure for OnceMap<K, V> {
    fn init(lf: GuardedLandfill) -> io::Result<Self> {
        let data = lf.substructure("data")?;
        let index = lf.substructure("index")?;

        Ok(OnceMap {
            data,
            index,
            _marker: PhantomData,
        })
    }

    fn flush(&self) -> io::Result<()> {
        self.data.flush()?;
        self.index.flush()
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
                    let key_bytes =
                        self.data.get(entry.k_ofs, mem::size_of::<K>() as u32);
                    let key_slice: &[K] = bytemuck::cast_slice(key_bytes);

                    if k == key_slice[0] {
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
                let k_as_slice = &[k];
                let k_as_bytes: &[u8] = bytemuck::cast_slice(k_as_slice);
                let k_ofs = self
                    .data
                    .write_aligned(k_as_bytes, mem::align_of::<K>())?;

                let v_as_slice = &[v];
                let v_as_bytes: &[u8] = bytemuck::cast_slice(v_as_slice);
                let v_ofs = self
                    .data
                    .write_aligned(v_as_bytes, mem::align_of::<V>())?;
                let v_ofs_relative = (v_ofs - k_ofs) as u32;

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
                let key_bytes =
                    self.data.get(entry.k_ofs, mem::size_of::<K>() as u32);
                let key_slice: &[K] = bytemuck::cast_slice(key_bytes);

                if &key_slice[0] == k {
                    // found it!
                    let v_ofs = entry.k_ofs + entry.v_ofs_relative as u64;
                    let v_bytes =
                        self.data.get(v_ofs, mem::size_of::<V>() as u32);
                    let v_slice: &[V] = bytemuck::cast_slice(v_bytes);
                    result = Some(&v_slice[0]);
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
