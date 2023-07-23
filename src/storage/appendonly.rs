use std::io;

use bytemuck_derive::*;

use super::bytes::DiskBytes;
use crate::Journal;
use crate::Landfill;

/// AppendOnly
///
/// An unbounded slice of bytes, that can only grow.
///
/// Since the collection can only grow, and written bytes never move in memory,
/// it is possible to keep shared references into the stored bytes, while still
/// concurrently appending new data.
pub struct AppendOnly<const INIT_SIZE: u64> {
    bytes: DiskBytes<INIT_SIZE>,
    journal: Journal<u64, 1024>,
}

/// A record of data put into the `AppendOnly` store
#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct Record {
    offset: u64,
    length: u32,
    _pad: u32,
}

impl<const INIT_SIZE: u64> TryFrom<&Landfill> for AppendOnly<INIT_SIZE> {
    type Error = io::Error;
    fn try_from(landfill: &Landfill) -> io::Result<AppendOnly<INIT_SIZE>> {
        let landfill = landfill.branch("ao");
        let bytes = DiskBytes::try_from(&landfill)?;
        let journal = Journal::try_from(&landfill)?;

        Ok(AppendOnly { bytes, journal })
    }
}

impl<const INIT_SIZE: u64> AppendOnly<INIT_SIZE> {
    /// Flush the data to disk
    ///
    /// This function blocks until completion
    pub fn flush(&self) -> io::Result<()> {
        self.bytes.flush()
    }

    /// Write bytes to the store.
    /// Returns a `Record` of the written data
    pub fn write(&self, bytes: &[u8]) -> io::Result<Record> {
        let len = bytes.len();

        let write_offset = self.journal.update(|writehead| {
            let res = DiskBytes::<INIT_SIZE>::find_space_for(*writehead, len);
            *writehead = res + len as u64;
            res
        })?;

        let slice = unsafe { self.bytes.request_write(write_offset, len)? };
        slice.copy_from_slice(bytes);

        let record = Record {
            offset: write_offset,
            length: len as u32,
            _pad: 0xffffffff,
        };

        Ok(record)
    }

    /// Get a reference to the data associated with the provided `Record`
    pub fn get(&self, record: Record) -> &[u8] {
        self.bytes
            .read(record.offset, record.length)
            .expect("Fatal Error: invalid record!")
    }
}
