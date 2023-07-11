use std::marker::PhantomData;
use std::path::Path;
use std::{fs, io};

use blake3::{traits::digest::Digest, Hasher as Blake3};

mod contentid;
pub use contentid::ContentId;

mod diskbytes;

mod journal;

use diskbytes::journaled::JournaledBytes;

mod header;
use header::Header;

mod index;
use index::Index;

pub struct LandFill<D> {
    index: Index,
    data: JournaledBytes<{ 1024 * 4 }>,
    _marker: PhantomData<D>,
}

impl<D> LandFill<D>
where
    D: Digest,
{
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        println!("creating {:?}", path.as_ref());

        fs::create_dir_all(&path)?;

        let header = Header::open(&path)?;

        let index = Index::open(&path, header)?;
        let data = JournaledBytes::open(&path, header)?;

        Ok(LandFill {
            index,
            data,
            _marker: PhantomData,
        })
    }

    pub fn insert_aligned(
        &self,
        bytes: &[u8],
        _alignment: usize,
    ) -> io::Result<ContentId> {
        let len = bytes.len();
        let id = ContentId::hash_bytes::<D>(bytes);

        self.index.find_matching_or_new(
            id,
            |ofs, match_len| {
                if match_len == len
                    && self.data.read(ofs, match_len) == Some(bytes)
                {
                    // we already have the data
                    Ok(true)
                } else {
                    Ok(false)
                }
            },
            |mut vacant| {
                let (ofs, target) = self.data.request_write(len)?;
                target.copy_from_slice(bytes);

                println!(
                    "target written {:?}",
                    String::from_utf8_lossy(target)
                );

                *vacant = index::TreeSlot::new(
                    ofs as u64,
                    len as u32,
                    id.discriminant(),
                );
                Ok(())
            },
        )?;

        Ok(id)
    }

    pub fn insert(&self, bytes: &[u8]) -> io::Result<ContentId> {
        self.insert_aligned(bytes, 1)
    }

    pub fn get(&self, id: ContentId) -> Option<&[u8]> {
        self.index.find_matching(id, |ofs, len| {
            if let Some(bytes) = self.data.read(ofs, len) {
                let stored_bytes_id = ContentId::hash_bytes::<D>(bytes);
                if stored_bytes_id == id {
                    Some(bytes)
                } else {
                    None
                }
            } else {
                None
            }
        })
    }
}

pub type Db = LandFill<Blake3>;

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;
    use tempfile;

    fn with_temp_path<R, F>(mut f: F) -> io::Result<R>
    where
        F: FnMut(&Path) -> io::Result<R>,
    {
        let dir = tempfile::tempdir()?;
        let path = PathBuf::from(dir.path());
        f(path.as_ref())
    }

    #[test]
    fn trivial_insert_read() -> io::Result<()> {
        with_temp_path(|path| {
            let db = Db::open(path)?;

            let message = b"hello world";

            let id = db.insert(message)?;

            println!("agubi");

            assert_eq!(
                id,
                ContentId::from_hex(
                    "d74981efa70a0c880b8d8c1985d075dbcbf679b99a5f9914e5aaf96b831a9e24",
                ),
            );

            let back = db.get(id);

            assert_eq!(back, Some(&message[..]));

            Ok(())
        })
    }

    const N: usize = 2;

    #[test]
    fn multiple() -> io::Result<()> {
        with_temp_path(|path| {
            let db = Db::open(path)?;

            let mut ids = vec![];

            for i in 0..N {
                let string = format!("hello world! {}", i);
                ids.push(db.insert(string.as_bytes())?);
            }

            for i in 0..N {
                let id = ids[i];
                assert_eq!(
                    db.get(id),
                    Some(format!("hello world! {}", i).as_bytes())
                )
            }

            Ok(())
        })
    }
}
