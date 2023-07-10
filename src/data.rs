use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use arr_macro::arr;
use memmap2::{Mmap, MmapOptions};

const MAP_BYTES_MIN: usize = 1024; // map minimum a gigabyte
const N_LANES: usize = 32;
const LANE_BASE_SIZE: usize = 1024 * 1024;

use crate::journal::Journal;

pub struct Data {
    file: File,
    maps: [OnceLock<Mmap>; N_LANES],
}

impl Data {
    pub(crate) fn open<P: AsRef<Path>>(
        path: P,
        journal: Journal,
    ) -> io::Result<Data> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("data");

        println!("opening {:?}", pb);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&pb)?;

        let bytes_written = journal.bytes_written();

        file.set_len(bytes_written)?;

        // initialize memory maps

        let mut lane = 0;
        let mut offset = 0;
        let mut size = LANE_BASE_SIZE;
        let mut left_to_map =
            std::cmp::max(bytes_written, LANE_BASE_SIZE as u64);

        let maps: [OnceLock<Mmap>; N_LANES] = arr![OnceLock::default(); 32];

        while left_to_map > 0 {
            let map = unsafe {
                MmapOptions::new().len(size).offset(offset).map(&file)?
            };
            maps[lane]
                .set(map)
                .expect("only one call site, data races impossible");

            left_to_map = left_to_map.saturating_sub(size as u64);
            offset += size as u64;
            size = size * 2;
        }

        Ok(Data { file, maps })
    }

    pub(crate) fn write(&self, data: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_all_at(data, offset)?;
        let _written_size = offset + data.len() as u64;

        Ok(())
    }

    pub(crate) fn read(&self, ofs: u64, len: u32) -> &[u8] {
        &self.maps[0]
            .get()
            .map(|m| &m[ofs as usize..][..len as usize])
            .unwrap_or_default()
    }
}
