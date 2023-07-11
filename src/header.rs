use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::{
    io::{self, Write},
    mem,
};

use bytemuck::{Pod, Zeroable};
use rand::{thread_rng, Rng};
use seahash::SeaHasher;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub(crate) struct Header {
    magic: [u8; 4],
    version: u32,
    key_a: u64,
    key_b: u64,
    key_c: u64,
    key_d: u64,
}

unsafe impl Zeroable for Header {}
unsafe impl Pod for Header {}

impl Header {
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> io::Result<Header> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("header");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&pb)?;

        file.set_len(mem::size_of::<Header>() as u64)?;

        let mut header = Header::zeroed();
        let header_unislice = &mut [header];
        let header_bytes: &mut [u8] = bytemuck::cast_slice_mut(header_unislice);

        file.read_at(header_bytes, 0)?;

        if header == Header::zeroed() {
            let mut rng = thread_rng();
            header = Header {
                magic: *b"lnfl",
                version: 1,
                key_a: rng.gen(),
                key_b: rng.gen(),
                key_c: rng.gen(),
                key_d: rng.gen(),
            };

            file.write_all(bytemuck::cast_slice(&[header]))?;
        }

        Ok(header)
    }

    pub fn checksum<T: Hash>(&self, t: T) -> u64 {
        let mut hasher = SeaHasher::with_seeds(
            self.key_a, self.key_b, self.key_c, self.key_d,
        );
        t.hash(&mut hasher);
        hasher.finish()
    }
}
