use std::io;
use std::path::{Path, PathBuf};

use bytemuck_derive::*;

use super::bytes::DiskBytes;
use crate::Journal;
use crate::{Entropy, Tag};

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
    tag: Tag,
}

/// A record of data put into the `AppendOnly` store
///
/// This functions as a receit of sorts, and is the only way to access the stored
/// data. The provided `Tag` is randomly generated, and not accessible for the API
/// user, and serves as a guarantee that the `Record` comes from the correct store.
#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct Record {
    offset: u64,
    length: u32,
    tag: Tag,
}

impl<const INIT_SIZE: u64> AppendOnly<INIT_SIZE> {
    /// Open an AppendOnly store at given path
    ///
    /// This call will create a directory `data` at the given path, and store
    /// its files in this location
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<AppendOnly<INIT_SIZE>> {
        let pb = PathBuf::from(path.as_ref());

        let entropy = Entropy::open(&pb)?;
        let bytes = DiskBytes::open(&pb)?;
        let journal = Journal::open(&pb)?;

        Ok(AppendOnly {
            bytes,
            journal,
            tag: entropy.tag(),
        })
    }

    /// Create an ephemeral `AppendOnly` backed by anonymous memory maps
    pub fn ephemeral() -> io::Result<AppendOnly<INIT_SIZE>> {
        let entropy = Entropy::ephemeral();
        let bytes = DiskBytes::ephemeral()?;
        let journal = Journal::ephemeral();

        Ok(AppendOnly {
            bytes,
            journal,
            tag: entropy.tag(),
        })
    }

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
            tag: self.tag,
        };

        Ok(record)
    }

    /// Get a reference to the data associated with the provided `Record`
    pub fn get(&self, record: Record) -> &[u8] {
        assert!(
            record.tag == self.tag,
            "Fatal Error: Usage of record issued in another path!"
        );
        self.bytes
            .read(record.offset, record.length)
            .expect("Fatal Error: invalid record!")
    }
}
