use std::fs;
use std::io;
use std::mem;
use std::path::{Path, PathBuf};

use bytemuck::{Pod, Zeroable};

use crate::contentid::ContentId;
use crate::header::Header;

const FANOUT: usize = 1024 * 4;

use crate::diskbytes::writeonce::{Initialize, WriteOnceArray};

/// A slot representing a value,
#[allow(unused)]
#[repr(C, align(32))]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TreeSlot {
    ofs: u64,          // 8 bytes
    len: u32,          // 4 bytes
    discriminant: u32, // 4 bytes from the contentid
}

unsafe impl Zeroable for TreeSlot {}
unsafe impl Pod for TreeSlot {}

impl TreeSlot {
    pub fn new(ofs: u64, len: u32, discriminant: u32) -> Self {
        TreeSlot {
            ofs,
            len,
            discriminant,
        }
    }
}

pub struct Index {
    slots: WriteOnceArray<{ FANOUT * mem::size_of::<TreeSlot>() }, TreeSlot>,
    header: Header,
}

impl Index {
    pub(crate) fn open<P: AsRef<Path>>(
        path: P,
        header: Header,
    ) -> io::Result<Index> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("index");
        fs::create_dir_all(&pb)?;

        let slots = WriteOnceArray::open(pb)?;

        Ok(Index { header, slots })
    }

    pub(crate) fn find_matching_or_new<F, N>(
        &self,
        id: ContentId,
        found: F,
        new: N,
    ) -> io::Result<()>
    where
        F: Fn(usize, usize) -> io::Result<bool>,
        N: Fn(Initialize<TreeSlot>) -> io::Result<()>,
    {
        let mut base: u64 = 0;

        let mut entropy = self.header.checksum(id);
        let disc = id.discriminant();

        let mut fanout = FANOUT as u64;

        loop {
            let slot_index = (base + (entropy % fanout)) as usize;

            if let Some(slot) = self.slots.get_nonzero(slot_index) {
                if slot.discriminant == disc {
                    if found(slot.ofs as usize, slot.len as usize)? {
                        return Ok(());
                    }
                }
            } else {
                if let Some(vacant) = self.slots.initialize(slot_index)? {
                    new(vacant)?;
                    return Ok(());
                } else {
                    // already written to, restart loop
                    continue;
                }
            }

            base += fanout;
            fanout *= 2;
            entropy = self.header.checksum(entropy);
        }
    }

    pub fn find_matching<F, R>(&self, id: ContentId, found: F) -> Option<R>
    where
        F: Fn(usize, usize) -> Option<R>,
    {
        let mut base: u64 = 0;

        let mut entropy = self.header.checksum(id);
        let disc = id.discriminant();

        let mut fanout = FANOUT as u64;

        loop {
            let slot_index = (base + (entropy % fanout)) as usize;

            if let Some(slot) = self.slots.get_nonzero(slot_index) {
                if slot.discriminant == disc {
                    if let Some(result) =
                        found(slot.ofs as usize, slot.len as usize)
                    {
                        return Some(result);
                    }
                }
            } else {
                return None;
            }

            base += fanout;
            fanout *= 2;
            entropy = self.header.checksum(entropy);
        }
    }
}
