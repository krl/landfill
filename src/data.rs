use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;
use parking_lot::Mutex;

pub struct Data {
    file: File,
    map: MmapMut,
    writehead: Mutex<u64>,
}

impl Data {
    pub(crate) fn open<P: AsRef<Path>>(path: P, writehead: u64) -> io::Result<Data> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("index");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&pb)?;

        let map = unsafe { MmapMut::map_mut(&file)? };

        Ok(Data {
            file,
            map,
            writehead: Mutex::new(writehead),
        })
    }

    /// Writes the data to its file, returning the offset at which it was written
    pub(crate) fn write(&self, _data: &[u8]) -> io::Result<u64> {
        todo!()
    }

    pub(crate) fn read(&self, ofs: u64, len: u32) -> &[u8] {
        todo!()
    }
}
