use std::fs::{File, OpenOptions};
use std::io::{self};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytemuck::{Pod, Zeroable};
use memmap2::MmapMut;
use parking_lot::Mutex;

use crate::header::Header;

const JOURNAL_LEN: usize = 16;

#[derive(Clone, Copy)]
#[repr(C)]
struct JournalEntry {
    nodes_reserved: u32,
    bytes_written: u64,
    checksum: u64,
}

impl JournalEntry {
    fn new(nodes_reserved: u32, bytes_written: u64, header: Header) -> Self {
        let checksum = header.checksum((nodes_reserved, bytes_written));
        JournalEntry {
            nodes_reserved,
            bytes_written,
            checksum,
        }
    }

    fn get(&self, header: Header) -> Option<(u32, u64)> {
        let checksum =
            header.checksum((self.nodes_reserved, self.bytes_written));
        if checksum == self.checksum {
            Some((self.nodes_reserved, self.bytes_written))
        } else {
            None
        }
    }
}

unsafe impl Zeroable for JournalEntry {}
unsafe impl Pod for JournalEntry {}

struct JournalInner {
    file: File,
    map: MmapMut,
    latest_entry_index: usize,
    header: Header,
}

#[derive(Clone)]
pub(crate) struct Journal(Arc<Mutex<JournalInner>>);

impl Journal {
    pub(crate) fn open<P: AsRef<Path>>(
        path: P,
        header: Header,
    ) -> io::Result<Journal> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("journal");

        println!("opening {:?}", pb);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&pb)?;

        const JOURNAL_BYTES: usize =
            std::mem::size_of::<JournalEntry>() * JOURNAL_LEN;

        file.set_len(JOURNAL_BYTES as u64)?;

        let mut map = unsafe { MmapMut::map_mut(&file)? };

        let journal_entry_slice = map.as_mut();
        let journal_entries: &mut [JournalEntry] =
            &mut bytemuck::cast_slice_mut(journal_entry_slice);

        assert_eq!(journal_entries.len(), JOURNAL_LEN);

        let mut latest_entry_index = 0;
        let mut candidate = (0, 0);

        for i in 0..JOURNAL_LEN {
            if let Some((a, b)) = journal_entries[i].get(header) {
                if (a, b) > candidate {
                    latest_entry_index = i;
                    candidate = (a, b);
                }
            }
        }

        if candidate == (0, 0) {
            journal_entries[0] = JournalEntry::new(1, 0, header)
        }

        let inner = JournalInner {
            file,
            map,
            latest_entry_index,
            header,
        };

        Ok(Journal(Arc::new(Mutex::new(inner))))
    }

    pub fn reserve_data_bytes(&self, len: u32, _alignment: usize) -> u64 {
        self.0.lock().reserve_data_bytes(len, _alignment)
    }

    pub fn reserve_node(&self) -> u32 {
        self.0.lock().reserve_node()
    }

    pub fn bytes_written(&self) -> u64 {
        self.0.lock().bytes_written()
    }

    pub fn nodes_reserved(&self) -> u32 {
        self.0.lock().nodes_reserved()
    }
}

impl JournalInner {
    fn update_entry(&mut self, nodes_reserved: u32, bytes_written: u64) {
        let entries: &mut [JournalEntry] =
            bytemuck::cast_slice_mut(&mut self.map[..]);

        let next_entry = (self.latest_entry_index + 1) % JOURNAL_LEN;
        let checksum = self.header.checksum((nodes_reserved, bytes_written));
        entries[next_entry] = JournalEntry {
            nodes_reserved,
            bytes_written,
            checksum,
        };
        self.latest_entry_index = next_entry;
    }

    // reserve n bytes
    // TODO: make sure to not land on DATA SEGMENT BORDERS and respect alignment
    //
    // NB: Returns the _old_ value, i.e the start of the reserved section,
    // as opposed to `reserve_node` that returns the _new_ node index
    fn reserve_data_bytes(&mut self, len: u32, _alignment: usize) -> u64 {
        let JournalEntry {
            nodes_reserved,
            bytes_written,
            ..
        } = self.current_entry().clone();

        let new_bytes_written = bytes_written + len as u64;
        self.update_entry(nodes_reserved, new_bytes_written);
        bytes_written
    }

    fn reserve_node(&mut self) -> u32 {
        let JournalEntry {
            nodes_reserved,
            bytes_written,
            ..
        } = self.current_entry().clone();

        let new_nodes_reserved = nodes_reserved + 1;
        self.update_entry(new_nodes_reserved, bytes_written);
        new_nodes_reserved
    }

    fn current_entry(&self) -> &JournalEntry {
        let entries: &[JournalEntry] = bytemuck::cast_slice(&self.map[..]);
        &entries[self.latest_entry_index]
    }

    fn bytes_written(&self) -> u64 {
        self.current_entry().bytes_written
    }

    fn nodes_reserved(&self) -> u32 {
        self.current_entry().nodes_reserved
    }
}
