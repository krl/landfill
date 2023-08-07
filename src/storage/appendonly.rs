use std::io;

use super::bytes::DiskBytes;
use crate::{GuardedLandfill, Journal, Substructure};

/// AppendOnly
/// Since the collection can only grow, and written bytes never move in memory,
/// it is possible to keep shared references into the stored bytes, while still
/// concurrently appending new data.
pub struct AppendOnly {
    bytes: DiskBytes,
    journal: Journal<u64>,
}

impl Substructure for AppendOnly {
    fn init(lf: GuardedLandfill) -> io::Result<AppendOnly> {
        let bytes = lf.substructure("bytes")?;
        let journal = lf.substructure("journal")?;

        Ok(AppendOnly { bytes, journal })
    }

    fn flush(&self) -> io::Result<()> {
        self.bytes.flush()
    }
}

impl AppendOnly {
    /// Write a slice of bytes into the store returning their offset
    pub fn write_aligned(
        &self,
        bytes: &[u8],
        alignment: usize,
    ) -> io::Result<u64> {
        let len = bytes.len();

        let write_offset = self.journal.update(|writehead| {
            let res = DiskBytes::find_space_for(*writehead, len, alignment);
            *writehead = res + len as u64;
            res
        });

        let slice = unsafe { self.bytes.request_write(write_offset, len)? };

        slice.copy_from_slice(bytes);

        Ok(write_offset)
    }

    /// Write a slice of bytes into the store returning their offset
    pub fn write(&self, bytes: &[u8]) -> io::Result<u64> {
        self.write_aligned(bytes, 1)
    }

    /// Get a reference to the data at offset and length
    pub fn get(&self, offset: u64, len: u32) -> &[u8] {
        self.bytes
            .read(offset, len)
            .expect("Fatal Error: invalid offset or length!")
    }
}
