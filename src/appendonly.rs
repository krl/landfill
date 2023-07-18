use std::io;
use std::path::{Path, PathBuf};

use crate::bytes::DiskBytes;
use crate::journal::Journal;

pub struct AppendOnly<const INIT_SIZE: u64> {
    bytes: DiskBytes<INIT_SIZE>,
    journal: Journal<u64, 1024>,
}

impl<const INIT_SIZE: u64> AppendOnly<INIT_SIZE> {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<AppendOnly<INIT_SIZE>> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("data");

        let bytes = DiskBytes::open(&pb)?;
        let journal = Journal::open(&pb)?;

        Ok(AppendOnly { bytes, journal })
    }

    pub fn flush(&self) -> io::Result<()> {
        todo!()
    }

    pub fn write(&self, bytes: &[u8]) -> io::Result<u64> {
        let len = bytes.len();

        let write_offset = self.journal.update(|writehead| {
            let res = self.bytes.find_space_for(*writehead, len);
            *writehead = res + len as u64;
            res
        })?;

        let slice = unsafe { self.bytes.request_write(write_offset, len)? };
        slice.copy_from_slice(bytes);
        Ok(write_offset)
    }

    pub fn read(&self, ofs: u64, len: u32) -> Option<&[u8]> {
        self.bytes.read(ofs, len)
    }
}
