use std::hash::{Hash, Hasher};
use std::io::{self};
use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable};
use bytemuck_derive::*;
use parking_lot::Mutex;
use seahash::SeaHasher;

use crate::{Landfill, MappedFile};

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

struct JournalInner<T, const SIZE: usize> {
    mapping: MappedFile,
    latest_entry_index: usize,
    _marker: PhantomData<T>,
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

impl<T, const SIZE: usize> TryFrom<&Landfill> for Journal<T, SIZE>
where
    T: Zeroable + Pod + Default + Hash + Ord,
{
    type Error = io::Error;

    fn try_from(landfill: &Landfill) -> io::Result<Self> {
        if let Some(mapping) =
            landfill.map_file_create("journal", SIZE as u64)?
        {
            let journal_entry_slice = unsafe { mapping.bytes_mut() };
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

            Ok(Journal(Mutex::new(JournalInner {
                mapping,
                latest_entry_index,
                _marker: PhantomData,
            })))
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Attempt at mapping the same file twice",
            ))
        }
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
        let entries: &mut [JournalEntry<T>] =
            bytemuck::cast_slice_mut(unsafe { self.mapping.bytes_mut() });
        let entry = &mut entries[self.latest_entry_index];

        let mut value = entry.value;
        let old_value = entry.value;

        let next_entry = (self.latest_entry_index + 1) % SIZE;

        let res = f(&mut value);

        assert!(value >= old_value, "Journal updates must be incremental");

        entries[next_entry] = JournalEntry::new(value);
        self.mapping.flush()?;
        self.latest_entry_index = next_entry;
        Ok(res)
    }
}
