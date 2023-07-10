use std::cell::UnsafeCell;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::{io, mem};

use arr_macro::arr;
use bytemuck::{Pod, Zeroable};
use memmap2::MmapMut;
use parking_lot::{Mutex, MutexGuard};

use crate::CheckSummer;
use crate::ContentId;

const FANOUT: usize = 256 * 256;
const N_LOCKS: usize = 1024;

/// A slot representing a value,
#[allow(unused)]
#[repr(C, align(32))]
#[derive(Clone, Copy, PartialEq, Eq)]
struct TreeSlot {
    ofs: u64,                // 8 bytes
    len: u32,                // 4 bytes
    discriminant: u32,       // 4 bytes from the contentid
    next_node_nr: u32,       // 4 bytes
    next_node_checksum: u32, // 4 bytes
}

unsafe impl Zeroable for TreeSlot {}
unsafe impl Pod for TreeSlot {}

#[repr(C, align(4096))]
#[derive(Clone, Copy)]
struct TreeNode([TreeSlot; FANOUT]);

unsafe impl Zeroable for TreeNode {}
unsafe impl Pod for TreeNode {}

pub(crate) struct TreeInsert<'a> {
    slot: &'a mut TreeSlot,
    guard: MutexGuard<'a, ()>,
}

impl<'a> TreeInsert<'a> {
    pub(crate) fn record(&mut self, offset: u64, len: u32, disc: u32) {
        self.slot.ofs = offset;
        self.slot.len = len;
        self.slot.discriminant = disc;
    }
}

pub(crate) enum CheckSlot<'a> {
    MatchingDiscriminant { ofs: u64, len: u32 },
    Vacant(TreeInsert<'a>),
}

pub struct Index {
    file: File,
    map: UnsafeCell<MmapMut>,
    chk: CheckSummer,
    writelocks: [Mutex<()>; N_LOCKS],
}

impl Index {
    pub(crate) fn open<P: AsRef<Path>>(
        path: P,
        n_pages: u32,
        chk: CheckSummer,
    ) -> io::Result<Index> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("index");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&pb)?;

        file.set_len((n_pages as usize * mem::size_of::<TreeNode>()) as u64)?;

        let map = unsafe { UnsafeCell::new(MmapMut::map_mut(&file)?) };

        let writelocks = arr![Default::default(); 1024];

        Ok(Index {
            file,
            map,
            chk,
            writelocks,
        })
    }

    pub(crate) fn insert<C>(&self, id: ContentId, check_if_found: C) -> io::Result<()>
    where
        C: Fn(CheckSlot) -> io::Result<bool>,
    {
        let discriminant = id.discriminant();

        let mut entropy = self.chk.checksum(id);

        let map = unsafe { &mut *self.map.get() };

        let root_node_slice = &mut map[..mem::size_of::<TreeNode>()];
        let root_node: &mut TreeNode = &mut bytemuck::cast_slice_mut(root_node_slice)[0];
        let mut current_node = root_node;

        let slot_nr = (entropy % FANOUT as u64) as usize;
        let lock_nr = (entropy % N_LOCKS as u64) as usize;
        let slot = &mut current_node.0[slot_nr];

        loop {
            if slot == &TreeSlot::zeroed()
                && check_if_found(CheckSlot::Vacant(TreeInsert {
                    slot,
                    guard: self.writelocks[lock_nr].lock(),
                }))?
            {
                return Ok(());
            };

            if slot.discriminant == discriminant
                && check_if_found(CheckSlot::MatchingDiscriminant {
                    ofs: slot.ofs,
                    len: slot.len,
                })?
            {
                return Ok(());
            }

            if slot.next_node_nr != 0
                && self.chk.checksum_truncated(&slot.next_node_nr) == slot.next_node_checksum
            {
                // reshuffle & follow to next page
                entropy = entropy.wrapping_mul(entropy);
                current_node = self.get_node(slot.next_node_nr);
            }

            todo!("we need to create a new node!")
        }
    }

    pub(crate) fn find<'a, C>(&'a self, id: ContentId, check_if_found: C) -> Option<&'a [u8]>
    where
        C: Fn(u64, u32) -> Option<&'a [u8]>,
    {
        let discriminant = id.discriminant();

        let mut entropy = self.chk.checksum(id);

        let map = unsafe { &mut *self.map.get() };

        let root_node_slice = &mut map[..mem::size_of::<TreeNode>()];
        let root_node: &mut TreeNode = &mut bytemuck::cast_slice_mut(root_node_slice)[0];
        let mut current_node = root_node;

        let slot_nr = (entropy % FANOUT as u64) as usize;
        let slot = &mut current_node.0[slot_nr];

        loop {
            if slot.discriminant == discriminant {
                if let Some(bytes) = check_if_found(slot.ofs, slot.len) {
                    return Some(bytes);
                }
            }

            if slot.next_node_nr != 0
                && self.chk.checksum_truncated(&slot.next_node_nr) == slot.next_node_checksum
            {
                // reshuffle & follow to next page
                entropy = entropy.wrapping_mul(entropy);
                current_node = self.get_node(slot.next_node_nr);
            }

            todo!("we need to create a new node!")
        }
    }

    fn get_node(&self, _node_nr: u32) -> &mut TreeNode {
        todo!()
    }
}
