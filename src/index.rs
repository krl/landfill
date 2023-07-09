use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::{io, mem};

use arr_macro::arr;
use bytemuck::{Pod, Zeroable};
use memmap2::MmapMut;
use parking_lot::{Mutex, MutexGuard};

use crate::header::Header;
use crate::ContentId;

const FANOUT: usize = 256 * 256;
const N_LOCKS: usize = 1024;

/// A slot representing a value,
#[allow(unused)]
#[repr(C, align(32))]
#[derive(Clone, Copy)]
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

pub(crate) enum ContinueSearch {
    Yes,
    No,
}

pub struct Index {
    file: File,
    map: MmapMut,
    root_node: *mut TreeNode,
    writelocks: [Mutex<()>; N_LOCKS],
}

impl Index {
    pub(crate) fn open<P: AsRef<Path>>(path: P, n_pages: u32) -> io::Result<Index> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("index");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&pb)?;

        let node_size = mem::size_of::<TreeNode>();

        file.set_len((n_pages as usize * node_size) as u64)?;

        let mut map = unsafe { MmapMut::map_mut(&file)? };

        let node_size = mem::size_of::<TreeNode>();

        let root_node_slice = &mut map.as_mut()[..node_size];
        let root_node: &mut TreeNode = &mut bytemuck::cast_slice_mut(root_node_slice)[0];

        let root_erased: *mut TreeNode = unsafe { mem::transmute(root_node) };

        let writelocks = arr![Default::default(); 1024];

        Ok(Index {
            file,
            map,
            root_node: root_erased,
            writelocks,
        })
    }

    pub(crate) fn insert<C>(&self, id: ContentId, header: &Header, check: C)
    where
        C: FnMut(CheckSlot) -> io::Result<ContinueSearch>,
    {
        let discriminant = id.discriminant();

        let mut checksummer = header.checksummer();
        id.hash(&mut checksummer);

        let entropy = checksummer.finish();

        let node = self.root_node;

        loop {
            let slot_nr = (entropy % FANOUT as u64) as usize;
            let slot = unsafe { &mut (*node).0[slot_nr] };
            todo!()
        }

        todo!()
    }
}
