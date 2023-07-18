use std::io;
use std::path::{Path, PathBuf};

use bytemuck_derive::*;

use crate::bytes::DiskBytes;
use crate::entropy::{Entropy, Tag};
use crate::journal::Journal;

pub struct AppendOnly<const INIT_SIZE: u64> {
    bytes: DiskBytes<INIT_SIZE>,
    journal: Journal<u64, 1024>,
    tag: Tag,
}

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct Record {
    offset: u64,
    length: u32,
    tag: Tag,
}

impl<const INIT_SIZE: u64> AppendOnly<INIT_SIZE> {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<AppendOnly<INIT_SIZE>> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("data");

        let entropy = Entropy::open(&pb)?;
        let bytes = DiskBytes::open(&pb)?;
        let journal = Journal::open(&pb)?;

        Ok(AppendOnly {
            bytes,
            journal,
            tag: entropy.tag(),
        })
    }

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

    pub fn flush(&self) -> io::Result<()> {
        self.bytes.flush()
    }

    pub fn write(&self, bytes: &[u8]) -> io::Result<Record> {
        let len = bytes.len();

        let write_offset = self.journal.update(|writehead| {
            let res = self.bytes.find_space_for(*writehead, len);
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
