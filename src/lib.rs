use std::marker::PhantomData;
use std::path::Path;
use std::{fs, io};

use blake3::{traits::digest::Digest, Hasher as Blake3};

mod contentid;
pub use contentid::ContentId;

mod header;
use header::Header;

mod index;
use index::{CheckSlot, ContinueSearch, Index};

mod data;
use data::Data;

pub struct LandFill<D> {
    header: Header,
    index: Index,
    data: Data,
    _marker: PhantomData<D>,
}

impl<D> LandFill<D>
where
    D: Digest,
{
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        fs::create_dir_all(&path)?;

        let header = Header::open(&path)?;

        let (n_pages, bytes_written) = header.read_journal();

        let index = Index::open(&path, n_pages)?;
        let data = Data::open(&path, bytes_written)?;

        Ok(LandFill {
            header,
            index,
            data,
            _marker: PhantomData,
        })
    }

    fn _insert(&self, bytes: &[u8], id: ContentId, page: u32, alignment: usize) -> io::Result<()> {
        self.index.insert(id, &self.header, |check| match check {
            CheckSlot::MatchingDiscriminant { ofs, len } => {
                if self.data.read(ofs, len) == bytes {
                    Ok(ContinueSearch::No)
                } else {
                    Ok(ContinueSearch::Yes)
                }
            }
            CheckSlot::Vacant(slot) => todo!(),
        });

        Ok(())
    }

    pub fn insert(&self, bytes: &[u8]) -> io::Result<ContentId> {
        assert!(bytes.len() <= u32::MAX as usize);
        let output = D::digest(bytes);
        let id = ContentId::from_slice(output.as_ref());

        self._insert(bytes, id, 0, 1)?;

        Ok(id)
    }

    pub fn get(&self, _id: ContentId) -> Option<&[u8]> {
        todo!()
    }
}

pub type Db = LandFill<Blake3>;

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;
    use tempfile;

    fn with_temp_path<R, F: FnMut(&Path) -> io::Result<R>>(mut f: F) -> io::Result<R> {
        let dir = tempfile::tempdir()?;
        let mut path = PathBuf::from(dir.path());
        path.push("db");
        f(path.as_ref())
    }

    #[test]
    fn test() -> io::Result<()> {
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
}
