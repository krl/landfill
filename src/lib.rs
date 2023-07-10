use std::marker::PhantomData;
use std::path::Path;
use std::{fs, io};

use blake3::{traits::digest::Digest, Hasher as Blake3};

mod contentid;
pub use contentid::ContentId;

mod header;
use header::Header;

mod journal;
use journal::Journal;

mod index;
use index::{CheckSlot, Index};

mod data;
use data::Data;

pub struct LandFill<D> {
    header: Header,
    journal: Journal,
    index: Index,
    data: Data,
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

        let journal = Journal::open(&path, header)?;

        let index = Index::open(&path, header, journal.clone())?;
        let data = Data::open(&path, journal.clone())?;

        Ok(LandFill {
            header,
            journal,
            index,
            data,
            _marker: PhantomData,
        })
    }

    pub fn insert_aligned(
        &self,
        bytes: &[u8],
        alignment: usize,
    ) -> io::Result<ContentId> {
        assert!(bytes.len() <= u32::MAX as usize);
        let id = ContentId::hash_bytes::<D>(bytes);

        self.index.insert(id, |check| match check {
            CheckSlot::MatchingDiscriminant { ofs, len } => {
                if self.data.read(ofs, len) == bytes {
                    // this value is already stored in the database
                    Ok(true) // found
                } else {
                    Ok(false) // continue searching
                }
            }
            CheckSlot::Vacant(mut slot) => {
                let len = bytes.len() as u32;
                let offset = self.journal.reserve_data_bytes(len, alignment);

                self.data.write(bytes, offset)?;
                slot.record(offset, len, id.discriminant());

                Ok(true)
            }
        })?;

        Ok(id)
    }

    pub fn insert(&self, bytes: &[u8]) -> io::Result<ContentId> {
        self.insert_aligned(bytes, 1)
    }

    pub fn get(&self, id: ContentId) -> Option<&[u8]> {
        self.index.find(id, |ofs, len| {
            let bytes = self.data.read(ofs, len);
            let stored_bytes_id = ContentId::hash_bytes::<D>(bytes);
            if stored_bytes_id == id {
                Some(bytes)
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

    fn with_temp_path<R, F: FnMut(&Path) -> io::Result<R>>(
        mut f: F,
    ) -> io::Result<R> {
        let dir = tempfile::tempdir()?;
        let mut path = PathBuf::from(dir.path());
        path.push("db");
        f(path.as_ref())
    }

    #[test]
    fn trivial() -> io::Result<()> {
        with_temp_path(|path| {
            let db = Db::open(path)?;

            let message = b"hello world";

            let id = db.insert(message)?;

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
