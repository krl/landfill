use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self};
use std::path::{Path, PathBuf};

use bytemuck::Pod;
use bytemuck_derive::*;
use memmap2::MmapMut;
use parking_lot::Mutex;
use seahash::SeaHasher;

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C, packed)]
struct JournalEntry<T> {
    checksum: u64,
    value: T,
}

impl<T> JournalEntry<T>
where
    T: Hash + Pod,
{
    #[inline(always)]
    fn checksum(value: &T) -> u64 {
        let mut hasher = SeaHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn new(value: T) -> Self {
        let checksum = Self::checksum(&value);
        JournalEntry { value, checksum }
    }

    fn get(&self) -> Option<T> {
        let value = self.value;
        if Self::checksum(&value) == self.checksum {
            Some(value)
        } else {
            None
        }
    }
}

enum JournalInner<T, const SIZE: usize> {
    Disk {
        _file: File,
        map: MmapMut,
        latest_entry_index: usize,
    },
    Mem(T),
}

/// A crash-resistant register of strictly incrementing values
///
/// Useful for keeping track of writeheads into other collections, specifically
/// `AppendOnly`
pub struct Journal<T, const SIZE: usize>(Mutex<JournalInner<T, SIZE>>);
impl<T, const SIZE: usize> Journal<T, SIZE>
where
    T: Pod + Clone + Hash + Ord + Default,
{
    /// Open or create a new journal at `path`, this is a single file and
    /// will not create any directories
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("journal");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&pb)?;

        file.set_len(SIZE as u64)?;

        let mut map = unsafe { MmapMut::map_mut(&file)? };

        let journal_entry_slice = map.as_mut();
        let journal_entries: &mut [JournalEntry<T>] =
            bytemuck::cast_slice_mut(journal_entry_slice);

        let mut latest_entry_index = 0;
        let mut candidate = T::default();

        for (i, entry) in journal_entries.iter().enumerate() {
            if let Some(val) = entry.get() {
                if val > candidate {
                    latest_entry_index = i;
                    candidate = val;
                }
            }
        }

        Ok(Journal(Mutex::new(JournalInner::Disk {
            _file: file,
            map,
            latest_entry_index,
        })))
    }

    /// Create an ephemeral `Journal`, note that this is nothing more than
    /// a mutex-wrapped `T`
    pub fn ephemeral() -> Self {
        Journal(Mutex::new(JournalInner::Mem(T::default())))
    }

    /// Takes a closure with mutable access to the guarded value
    ///
    /// PANICKING
    ///
    /// This method will panic if the updated value compares less as the old one,
    /// so make sure that it gets set equal to or greater than its old value.
    pub fn update<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        self.0.lock().update(f)
    }
}

impl<T, const SIZE: usize> JournalInner<T, SIZE>
where
    T: Pod + Clone + Hash + Ord,
{
    fn update<F, R>(&mut self, f: F) -> io::Result<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        match self {
            JournalInner::Disk {
                map,
                latest_entry_index,
                ..
            } => {
                let entries: &mut [JournalEntry<T>] =
                    bytemuck::cast_slice_mut(&mut map[..]);
                let entry = &mut entries[*latest_entry_index];

                let mut value = entry.value;
                let old_value = entry.value;

                let next_entry = (*latest_entry_index + 1) % SIZE;

                let res = f(&mut value);

                assert!(
                    value >= old_value,
                    "Journal updates must be incremental"
                );

                entries[next_entry] = JournalEntry::new(value);
                map.flush()?;
                *latest_entry_index = next_entry;
                Ok(res)
            }
            JournalInner::Mem(value) => {
                let value_copy = *value;
                let res = f(value);
                assert!(
                    *value > value_copy,
                    "Journal updates must be strictly incremental"
                );
                Ok(res)
            }
        }
    }
}
