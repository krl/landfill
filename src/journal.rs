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
    value: u64,
    checksum: u64,
}

impl JournalEntry {
    fn new(value: u64, header: Header) -> Self {
        let checksum = header.checksum(value);
        JournalEntry { value, checksum }
    }

    fn get(&self, header: Header) -> Option<u64> {
        let checksum = header.checksum(self.value);
        if checksum == self.checksum {
            Some(self.value)
        } else {
            None
        }
    }
}

unsafe impl Zeroable for JournalEntry {}
unsafe impl Pod for JournalEntry {}

struct JournalInner {
    #[allow(unused)]
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
        let mut candidate = 0;

        for i in 0..JOURNAL_LEN {
            if let Some(val) = journal_entries[i].get(header) {
                if val > candidate {
                    latest_entry_index = i;
                    candidate = val;
                }
            }
        }

        let inner = JournalInner {
            file,
            map,
            latest_entry_index,
            header,
        };

        Ok(Journal(Arc::new(Mutex::new(inner))))
    }

    pub fn update<F>(&self, f: F)
    where
        F: FnOnce(u64) -> u64,
    {
        self.0.lock().update(f);
    }
}

impl JournalInner {
    pub fn update<F>(&mut self, f: F)
    where
        F: FnOnce(u64) -> u64,
    {
        let old = self.read();
        let entries: &mut [JournalEntry] =
            bytemuck::cast_slice_mut(&mut self.map[..]);
        let next_entry = (self.latest_entry_index + 1) % JOURNAL_LEN;
        let new_value = f(old);
        entries[next_entry] = JournalEntry::new(new_value, self.header);
        self.latest_entry_index = next_entry;
    }

    fn current_entry(&self) -> &JournalEntry {
        let entries: &[JournalEntry] = bytemuck::cast_slice(&self.map[..]);
        &entries[self.latest_entry_index]
    }

    pub fn read(&self) -> u64 {
        self.current_entry().value
    }
}
