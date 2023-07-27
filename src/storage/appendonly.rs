use std::{io, mem};

use bytemuck::Pod;

use super::bytes::DiskBytes;
use crate::{Journal, Landfill};

/// AppendOnly
/// Since the collection can only grow, and written bytes never move in memory,
/// it is possible to keep shared references into the stored bytes, while still
/// concurrently appending new data.
pub struct AppendOnly {
    bytes: DiskBytes,
    journal: Journal<u64>,
}

impl TryFrom<&Landfill> for AppendOnly {
    type Error = io::Error;
    fn try_from(landfill: &Landfill) -> io::Result<AppendOnly> {
        let landfill = landfill.branch("ao");
        let bytes = DiskBytes::try_from(&landfill)?;
        let journal = Journal::try_from(&landfill)?;

        Ok(AppendOnly { bytes, journal })
    }
}

impl AppendOnly {
    /// Flush the data to disk
    ///
    /// This function blocks until completion
    pub fn flush(&self) -> io::Result<()> {
        self.bytes.flush()
    }

    /// Put a value into the Appendonly store, returning its ofset
    pub fn insert<T: Pod>(&self, t: T) -> io::Result<u64> {
        self.write(&[t])
    }

    /// Write a slice of values into the store returning their offset
    pub fn write<T: Pod>(&self, items: &[T]) -> io::Result<u64> {
        let len = items.len();
        let byte_size = len * mem::size_of::<T>();

        let write_offset = self.journal.update(|writehead| {
            let res = DiskBytes::find_space_for(
                *writehead,
                len,
                mem::align_of::<T>(),
            );
            *writehead = res + byte_size as u64;
            res
        });

        let slice =
            unsafe { self.bytes.request_write(write_offset, byte_size)? };

        let insert_bytes: &[u8] = bytemuck::cast_slice(items);

        slice.copy_from_slice(insert_bytes);

        Ok(write_offset)
    }

    /// Get a reference to the data at offset and length
    pub fn get<T>(&self, offset: u64) -> &T
    where
        T: Pod,
    {
        let bytes = self
            .bytes
            .read(offset, mem::size_of::<T>() as u32)
            .expect("Fatal Error: invalid offset or length!");

        &bytemuck::cast_slice(bytes)[0]
    }

    /// Get a reference to the data at offset and length
    pub fn get_slice<T>(&self, offset: u64, len: usize) -> &[T]
    where
        T: Pod,
    {
        let bytes = self
            .bytes
            .read(offset, (len * mem::size_of::<T>()) as u32)
            .expect("Fatal Error: invalid offset or length!");

        bytemuck::cast_slice(bytes)
    }
}
