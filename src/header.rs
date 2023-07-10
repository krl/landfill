use std::fs::{File, OpenOptions};
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::{
    io::{self, Write},
    mem,
};

use bytemuck::{Pod, Zeroable};
use memmap2::MmapMut;
use parking_lot::Mutex;
use rand::{thread_rng, Rng};

use crate::CheckSummer;

const JOURNAL_LEN: usize = 16;

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct StaticHeader {
    magic: [u8; 4],
    version: u32,
    pub(crate) key_a: u64,
    pub(crate) key_b: u64,
    pub(crate) key_c: u64,
    pub(crate) key_d: u64,
}

unsafe impl Zeroable for StaticHeader {}
unsafe impl Pod for StaticHeader {}

#[derive(Clone, Copy)]
#[repr(C)]
struct JournalEntry {
    n_nodes: u32,
    bytes_written: u64,
    checksum: u64,
}

impl JournalEntry {
    fn new(n_nodes: u32, bytes_written: u64, chk: &CheckSummer) -> Self {
        let checksum = chk.checksum((n_nodes, bytes_written));
        JournalEntry {
            n_nodes,
            bytes_written,
            checksum,
        }
    }

    fn get(&self, chk: &CheckSummer) -> Option<(u32, u64)> {
        let checksum = chk.checksum((self.n_nodes, self.bytes_written));
        if checksum == self.checksum {
            Some((self.n_nodes, self.bytes_written))
        } else {
            None
        }
    }
}

unsafe impl Zeroable for JournalEntry {}
unsafe impl Pod for JournalEntry {}

#[derive(Clone, Copy)]
#[repr(C)]
struct JournalEntries([JournalEntry; JOURNAL_LEN]);

impl JournalEntries {
    fn init(chk: &CheckSummer) -> Self {
        let entry = JournalEntry::new(1, 0, chk);
        let mut slf = Self::zeroed();
        slf.0[0] = entry;
        slf
    }
}

unsafe impl Zeroable for JournalEntries {}
unsafe impl Pod for JournalEntries {}

struct Journal {
    entries: *mut JournalEntries,
    entry_latest: usize,
    chk: CheckSummer,
}

impl Journal {
    fn from_entries(entries: &mut JournalEntries, chk: CheckSummer) -> Self {
        // Find the latest valid journal entry
        let mut entry_latest = 0;
        let mut candidate = (0, 0);

        let entries_slice = &entries.0[..];

        for i in 0..JOURNAL_LEN {
            if let Some((a, b)) = entries_slice[i].get(&chk) {
                if (a, b) > candidate {
                    entry_latest = i;
                    candidate = (a, b);
                }
            }
        }

        Journal {
            entries,
            entry_latest,
            chk,
        }
    }

    fn get_latest(&self) -> (u32, u64) {
        let entries = unsafe { &mut (*self.entries).0[..] };
        entries[self.entry_latest as usize]
            .get(&self.chk)
            .expect("memory corruption")
    }

    fn update_entry(&self, n_nodes: u32, bytes_written: u64) {
        let next_entry = (self.entry_latest + 1) % JOURNAL_LEN;
        let entries = unsafe { &mut (*self.entries).0[..] };
        let checksum = self.chk.checksum((n_nodes, bytes_written));
        entries[next_entry] = JournalEntry {
            n_nodes,
            bytes_written,
            checksum,
        }
    }

    fn reserve_data_bytes(&mut self, len: u32, _alignment: usize) -> u64 {
        let (n_nodes, old_bytes_written) = self.get_latest();
        let bytes_written = old_bytes_written + len as u64;
        self.update_entry(n_nodes, bytes_written);
        old_bytes_written
    }

    fn reserve_tree_node(&mut self) -> u32 {
        let (old_n_nodes, bytes_written) = self.get_latest();
        let n_nodes = old_n_nodes + 1;
        self.update_entry(n_nodes, bytes_written);
        old_n_nodes
    }
}

pub(crate) struct Header {
    file: File,
    map: MmapMut,
    chk: CheckSummer,
    journal: Mutex<Journal>,
}

impl Header {
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> io::Result<Header> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("header");

        // Let's create constants to be able to write ranges more cleanly later
        const STATIC_HEADER_SIZE: usize = mem::size_of::<StaticHeader>();
        const JOURNAL_ENTRIES_SIZE: usize = mem::size_of::<JournalEntries>();

        let (file, chk) = if pb.exists() {
            let file = OpenOptions::new().read(true).write(true).open(&pb)?;

            let mut static_header = StaticHeader::zeroed();
            let mut static_header_unislice = &mut [static_header];
            let mut static_header_bytes: &mut [u8] =
                bytemuck::cast_slice_mut(static_header_unislice);

            file.read_at(static_header_bytes, 0)?;

            let chk = CheckSummer::new_from_header(&static_header);

            (file, chk)
        } else {
            // initialize a new header
            let mut rg = thread_rng();
            let key_a: u64 = rg.gen();
            let key_b: u64 = rg.gen();
            let key_c: u64 = rg.gen();
            let key_d: u64 = rg.gen();

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&pb)?;

            // write the new header to the file

            // We use the pattern of the unary slice here to be able to cast to
            // &[u8] easily
            let static_header = StaticHeader {
                magic: *b"lnfl",
                version: 1,
                key_a,
                key_b,
                key_c,
                key_d,
            };
            let static_header_unislice = &[static_header];
            let static_header_bytes: &[u8] = bytemuck::cast_slice(static_header_unislice);

            let chk = CheckSummer::new(
                static_header.key_a,
                static_header.key_b,
                static_header.key_c,
                static_header.key_d,
            );

            let journal_entries_unislice = &[JournalEntries::init(&chk)];
            let journal_entries_bytes: &[u8] = bytemuck::cast_slice(journal_entries_unislice);

            file.write_all(static_header_bytes)?;
            file.write_all(journal_entries_bytes)?;

            (file, chk)
        };

        let mut map = unsafe { MmapMut::map_mut(&file)? };

        let journal_entry_slice = &mut map.as_mut()[STATIC_HEADER_SIZE..][..JOURNAL_ENTRIES_SIZE];
        let journal_entries: &mut JournalEntries =
            &mut bytemuck::cast_slice_mut(journal_entry_slice)[0];

        let journal = Mutex::new(Journal::from_entries(journal_entries, chk.clone()));

        Ok(Header {
            file,
            map,
            journal,
            chk,
        })
    }

    pub(crate) fn read_journal(&self) -> (u32, u64) {
        self.journal.lock().get_latest()
    }

    pub(crate) fn checksummer(&self) -> CheckSummer {
        self.chk.clone()
    }

    // reserve n bytes
    // TODO: make sure to not land on DATA SEGMENT BORDERS and respect alignment
    pub(crate) fn reserve_data_bytes(&self, len: u32, alignment: usize) -> io::Result<u64> {
        let mut journal = self.journal.lock();
        let ofs = journal.reserve_data_bytes(len, alignment);
        self.map.flush()?;
        Ok(ofs)
    }

    pub(crate) fn reserve_tree_node(&self) -> io::Result<u32> {
        let mut journal = self.journal.lock();
        let page_nr = journal.reserve_tree_node();
        self.map.flush()?;
        Ok(page_nr)
    }
}
