use std::io;
use std::path::{Path, PathBuf};

use crate::diskbytes::raw::DiskBytesRaw;
use crate::header::Header;
use crate::journal::Journal;

pub struct JournaledBytes<const INIT_SIZE: usize> {
    bytes: DiskBytesRaw<INIT_SIZE>,
    journal: Journal,
}

impl<const INIT_SIZE: usize> JournaledBytes<INIT_SIZE> {
    pub(crate) fn open<P: AsRef<Path>>(
        path: P,
        header: Header,
    ) -> io::Result<JournaledBytes<INIT_SIZE>> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("data");

        let bytes = DiskBytesRaw::open(&pb)?;
        let journal = Journal::open(&pb, header)?;

        Ok(JournaledBytes { bytes, journal })
    }

    pub(crate) fn request_write(
        &self,
        len: usize,
    ) -> io::Result<(usize, &mut [u8])> {
        let mut write_offset = 0;

        self.journal.update(|old_value| {
            write_offset = old_value as usize;
            loop {
                let free_space = self.bytes.bytes_left_at(write_offset);
                if free_space < len {
                    write_offset += free_space
                } else {
                    break;
                }
            }
            (write_offset + len) as u64
        });

        let write_buf = unsafe {
            self.bytes
                .write(write_offset, len)?
                .expect("We already checked for free space above")
        };

        Ok((write_offset, write_buf))
    }

    pub(crate) fn read(&self, ofs: usize, len: usize) -> Option<&[u8]> {
        self.bytes.read(ofs, len)
    }
}
