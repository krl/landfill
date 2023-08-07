use std::hash::Hash;
use std::io;
use std::marker::PhantomData;

use bytemuck_derive::*;
use digest::Digest;

use crate::{AppendOnly, GuardedLandfill, SmashMap, Substructure};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Entry {
    ofs: u64,
    len: u32,
    tag: u32,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable)]
pub struct ContentId([u8; 32]);

impl ContentId {
    fn from_bytes<D: Digest>(bytes: &[u8]) -> Self {
        let mut hash_bytes = [0u8; 32];
        let digest = D::digest(bytes);
        hash_bytes.copy_from_slice(digest.as_ref());
        ContentId(hash_bytes)
    }
}

/// A storage for content-adressable byte-slices
pub struct Content<D> {
    data: AppendOnly,
    index: SmashMap<ContentId, Entry>,
    _marker: PhantomData<D>,
}

impl<D> Substructure for Content<D> {
    fn init(lf: GuardedLandfill) -> io::Result<Self> {
        Ok(Content {
            data: lf.substructure("data")?,
            index: lf.substructure("index")?,
            _marker: PhantomData,
        })
    }

    fn flush(&self) -> io::Result<()> {
        self.data.flush()?;
        self.index.flush()
    }
}

impl<D> Content<D>
where
    D: Digest,
{
    /// Insert bytes into the Content store, returning the content id
    pub fn insert(&self, bytes: &[u8]) -> io::Result<ContentId> {
        self.insert_aligned(bytes, 1)
    }

    /// Insert bytes aligned to `alignment` into the Content store,
    /// returning the content id
    pub fn insert_aligned(
        &self,
        bytes: &[u8],
        alignment: usize,
    ) -> io::Result<ContentId> {
        let id = ContentId::from_bytes::<D>(bytes);

        self.index.insert(
            &id,
            |search, entry| {
                let search_tag = search.tag_u32();

                if search_tag == entry.tag {
                    let stored = self.data.get(entry.ofs, entry.len);
                    let stored_id = ContentId::from_bytes::<D>(stored);

                    if id == stored_id {
                        search.halt()
                    } else {
                        search.proceed()
                    }
                } else {
                    search.proceed()
                }
            },
            |search| {
                let ofs = self.data.write_aligned(bytes, alignment)?;

                Ok(Entry {
                    ofs,
                    len: bytes.len() as u32,
                    tag: search.tag_u32(),
                })
            },
        )?;
        Ok(id)
    }

    /// Gets the value corresponding to the key, if any
    pub fn get(&self, id: ContentId) -> Option<&[u8]> {
        let mut result = None;
        self.index.get(&id, |search, entry| {
            let search_tag = search.tag_u32();

            if search_tag == entry.tag {
                let stored = self.data.get(entry.ofs, entry.len);

                let stored_id = ContentId::from_bytes::<D>(stored);

                if stored_id == id {
                    // found it!
                    result = Some(stored);
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
