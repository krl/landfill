use std::{
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use bytemuck_derive::*;
use rand::Rng;
use seahash::SeaHasher;

#[repr(C)]
#[derive(Clone, Copy, Zeroable, Pod)]
pub struct Entropy([u64; 4]);

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Zeroable, Pod)]
pub struct Tag(u32);

impl Entropy {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut pb = PathBuf::from(path.as_ref());
        pb.push("entropy");

        match File::open(&pb) {
            Ok(mut file) => {
                let mut values = [0u64; 4];
                let as_bytes: &mut [u8] = bytemuck::cast_slice_mut(&mut values);
                file.read_exact(as_bytes)?;
                Ok(Entropy(values))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let mut file =
                    OpenOptions::new().write(true).create(true).open(&pb)?;
                let generated = &[Self::generate()];
                let as_bytes: &[u8] = bytemuck::cast_slice(generated);
                file.write_all(as_bytes)?;
                Ok(generated[0])
            }
            Err(e) => Err(e),
        }
    }

    pub fn ephemeral() -> Self {
        Self::generate()
    }

    fn generate() -> Self {
        let mut rng = rand::thread_rng();
        Entropy(rng.gen())
    }

    pub fn checksum<T: Hash>(&self, t: &T) -> u64 {
        let mut hasher =
            SeaHasher::with_seeds(self.0[0], self.0[1], self.0[2], self.0[3]);
        t.hash(&mut hasher);
        hasher.finish()
    }

    pub fn nonce(&self) -> u64 {
        rand::thread_rng().gen()
    }

    pub fn tag(&self) -> Tag {
        Tag(self.checksum(&()) as u32)
    }
}
