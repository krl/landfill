use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::{
    io::{self, Write},
    mem,
};

use bytemuck::{Pod, Zeroable};
use memmap2::MmapMut;
use parking_lot::Mutex;
use rand::{thread_rng, Rng};
use seahash::SeaHasher;

const JOURNAL_LEN: usize = 16;

#[derive(Clone, Copy)]
#[repr(C)]
struct StaticHeader {
    magic: [u8; 4],
    version: u32,
    key_a: u64,
    key_b: u64,
    key_c: u64,
    key_d: u64,
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
    fn new(n_nodes: u32, bytes_written: u64) -> Self {
        let mut checker = SeaHasher::new();
        n_nodes.hash(&mut checker);
        bytes_written.hash(&mut checker);
        let checksum = checker.finish();
        JournalEntry {
            n_nodes,
            bytes_written,
            checksum,
        }
    }

    fn get(&self) -> Option<(u32, u64)> {
        let mut check = SeaHasher::new();
        self.n_nodes.hash(&mut check);
        self.bytes_written.hash(&mut check);
        let cs = check.finish();
        if cs == self.checksum {
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
    fn init() -> Self {
        let entry = JournalEntry::new(1, 0);
        let mut slf = Self::zeroed();
        slf.0[0] = entry;
        slf
    }
}

unsafe impl Zeroable for JournalEntries {}
unsafe impl Pod for JournalEntries {}

struct Journal {
    entries: *mut JournalEntries,
    entry_latest: u8,
}

impl Journal {
    fn from_entries(entries: &mut JournalEntries) -> Self {
        // Find the latest valid journal entry
        let mut entry_latest = 0;
        let mut candidate = (0, 0);

        let entries_slice = &entries.0[..];

        for i in 0..JOURNAL_LEN {
            if let Some((a, b)) = entries_slice[i].get() {
                if (a, b) > candidate {
                    entry_latest = i as u8;
                    candidate = (a, b);
                }
            }
        }

        Journal {
            entries,
            entry_latest,
        }
    }

    fn get_latest(&self) -> (u32, u64) {
        let entries = unsafe { &mut (*self.entries).0[..] };
        entries[self.entry_latest as usize]
            .get()
            .expect("memory corruption")
    }
}

pub(crate) struct Header {
    file: File,
    map: MmapMut,
    static_header: *const StaticHeader,
    journal: Mutex<Journal>,
}

impl Header {
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> io::Result<Header> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("header");

        // Let's create constants to be able to write ranges more cleanly later
        const STATIC_HEADER_SIZE: usize = mem::size_of::<StaticHeader>();
        const JOURNAL_ENTRIES_SIZE: usize = mem::size_of::<JournalEntries>();

        let file = if pb.exists() {
            OpenOptions::new().read(true).write(true).open(&pb)?
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
            let static_header_unislice = &[StaticHeader {
                magic: *b"lnfl",
                version: 1,
                key_a,
                key_b,
                key_c,
                key_d,
            }];
            let static_header_bytes: &[u8] = bytemuck::cast_slice(static_header_unislice);

            let journal_entries_unislice = &[JournalEntries::init()];
            let journal_entries_bytes: &[u8] = bytemuck::cast_slice(journal_entries_unislice);

            file.write_all(static_header_bytes)?;
            file.write_all(journal_entries_bytes)?;

            file
        };

        let mut map = unsafe { MmapMut::map_mut(&file)? };

        let static_header_slice = &map.as_mut()[..STATIC_HEADER_SIZE];
        let static_header_ref: &StaticHeader = &bytemuck::cast_slice(static_header_slice)[0];
        let static_header: *const StaticHeader = unsafe { mem::transmute(static_header_ref) };

        let journal_entry_slice = &mut map.as_mut()[STATIC_HEADER_SIZE..][..JOURNAL_ENTRIES_SIZE];
        let journal_entries: &mut JournalEntries =
            &mut bytemuck::cast_slice_mut(journal_entry_slice)[0];

        let journal = Mutex::new(Journal::from_entries(journal_entries));

        Ok(Header {
            file,
            map,
            static_header,
            journal,
        })
    }

    pub(crate) fn read_journal(&self) -> (u32, u64) {
        self.journal.lock().get_latest()
    }

    pub(crate) fn checksummer(&self) -> SeaHasher {
        let a = unsafe { (*self.static_header).key_a };
        let b = unsafe { (*self.static_header).key_b };
        let c = unsafe { (*self.static_header).key_c };
        let d = unsafe { (*self.static_header).key_d };
        SeaHasher::with_seeds(a, b, c, d)
    }
}
