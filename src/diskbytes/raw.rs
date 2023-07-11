use arrayvec::ArrayVec;
use memmap2::MmapMut;
use std::cell::UnsafeCell;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

const N_LANES: usize = 32;

struct Mapping {
    #[allow(unused)]
    file: File,
    map: MmapMut,
}

impl Mapping {
    fn open<P: AsRef<Path>>(path: P, size: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(path.as_ref())?;

        file.set_len(size as u64)?;

        let map = unsafe { MmapMut::map_mut(&file)? };
        Ok(Mapping { file, map })
    }
}

impl Deref for Mapping {
    type Target = MmapMut;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DerefMut for Mapping {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

pub struct DiskBytesRaw<const INIT_SIZE: usize> {
    root_path: PathBuf,
    lanes: UnsafeCell<ArrayVec<Mapping, N_LANES>>,
}

impl<const INIT_SIZE: usize> DiskBytesRaw<INIT_SIZE> {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let pb = PathBuf::from(path.as_ref());
        fs::create_dir_all(&pb)?;

        let mut lanes = ArrayVec::new();

        for i in 0.. {
            let mut data_path = pb.clone();
            data_path.push(format!("{:02x}", i));

            if data_path.exists() {
                let mapping = Mapping::open(&data_path, INIT_SIZE)?;
                lanes.push(mapping);
            } else {
                break;
            }
        }

        Ok(DiskBytesRaw {
            root_path: pb,
            lanes: UnsafeCell::new(lanes),
        })
    }

    pub fn bytes_left_at(&self, offset: usize) -> usize {
        let (lane, offset) = Self::lane_and_ofs(offset);
        let lane_size = Self::lane_size(lane);
        lane_size - offset
    }

    pub unsafe fn write(
        &self,
        offset: usize,
        len: usize,
    ) -> io::Result<Option<&mut [u8]>> {
        let (lane, offset) = Self::lane_and_ofs(offset);
        let lane_size = Self::lane_size(lane);

        if offset + len > lane_size {
            // We cannot write in lane boundaries
            Ok(None)
        } else {
            let lanes = self.lanes.get();
            let lanes = unsafe { &mut *lanes };

            while lanes.len() <= lane {
                let i = lanes.len();
                let mut data_path = self.root_path.clone();
                data_path.push(format!("{:02x}", i));

                let mapping = Mapping::open(&data_path, Self::lane_size(i))?;
                lanes.push(mapping);
            }

            return Ok(Some(&mut lanes[lane][offset..][..len]));
        }
    }

    pub fn read(&self, offset: usize, len: usize) -> Option<&[u8]> {
        let (lane, offset) = Self::lane_and_ofs(offset);
        let lane_size = Self::lane_size(lane);

        if offset + len > lane_size {
            // We cannot read in lane boundaries
            None
        } else {
            let lanes = self.lanes.get();
            let lanes = unsafe { &*lanes };

            if let Some(mapping) = lanes.get(lane) {
                Some(&mapping[offset..][..len])
            } else {
                None
            }
        }
    }

    #[cfg(test)]
    fn lane_and_ofs_slow_but_obviously_correct(
        mut offset: usize,
    ) -> (usize, usize) {
        let mut lane = 0;

        loop {
            let lane_size = Self::lane_size(lane);
            if lane_size <= offset {
                lane += 1;
                offset -= lane_size;
            } else {
                return (lane, offset);
            }
        }
    }

    fn lane_and_ofs(offset: usize) -> (usize, usize) {
        let usize_bits = mem::size_of::<usize>() * 8;
        let i = offset / INIT_SIZE + 1;
        let lane = usize_bits - i.leading_zeros() as usize - 1;
        let page = offset - (2usize.pow(lane as u32) - 1) * INIT_SIZE;
        (lane, page)
    }

    fn lane_size(lane: usize) -> usize {
        INIT_SIZE * 2usize.pow(lane as u32)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;
    use tempfile;

    fn with_temp_path<R, F>(f: F) -> io::Result<R>
    where
        F: Fn(&Path) -> io::Result<R>,
    {
        let dir = tempfile::tempdir()?;
        let path = PathBuf::from(dir.path());
        f(path.as_ref())
    }

    #[test]
    fn test_lane_math_trivial() {
        assert_eq!(DiskBytesRaw::<32>::lane_and_ofs(0), (0, 0));
        assert_eq!(DiskBytesRaw::<32>::lane_and_ofs(31), (0, 31));
        assert_eq!(DiskBytesRaw::<32>::lane_and_ofs(32), (1, 0));
        assert_eq!(DiskBytesRaw::<32>::lane_and_ofs(64), (1, 32));
        assert_eq!(DiskBytesRaw::<32>::lane_and_ofs(95), (1, 63));
        assert_eq!(DiskBytesRaw::<32>::lane_and_ofs(96), (2, 0));
    }

    #[test]
    fn test_lane_math() {
        for i in 0..1024 * 1024 {
            assert_eq!(
                DiskBytesRaw::<32>::lane_and_ofs(i),
                DiskBytesRaw::<32>::lane_and_ofs_slow_but_obviously_correct(i),
            );

            assert_eq!(
                DiskBytesRaw::<1>::lane_and_ofs(i),
                DiskBytesRaw::<1>::lane_and_ofs_slow_but_obviously_correct(i),
            );

            assert_eq!(
                DiskBytesRaw::<1024>::lane_and_ofs(i),
                DiskBytesRaw::<1024>::lane_and_ofs_slow_but_obviously_correct(
                    i
                ),
            );

            assert_eq!(
                DiskBytesRaw::<17>::lane_and_ofs(i),
                DiskBytesRaw::<17>::lane_and_ofs_slow_but_obviously_correct(i),
            );
        }
    }

    #[test]
    fn test_lane_sizes() {
        assert_eq!(DiskBytesRaw::<32>::lane_size(0), 32);
        assert_eq!(DiskBytesRaw::<32>::lane_size(1), 64);
        assert_eq!(DiskBytesRaw::<32>::lane_size(2), 128);

        assert_eq!(DiskBytesRaw::<1024>::lane_size(0), 1024);
        assert_eq!(DiskBytesRaw::<1024>::lane_size(1), 2048);
        assert_eq!(DiskBytesRaw::<1024>::lane_size(2), 4096);
    }

    #[test]
    fn simple_write_read() -> io::Result<()> {
        with_temp_path(|path| {
            let da = DiskBytesRaw::<1024>::open(&path)?;

            let msg = b"hello world";
            let len = msg.len();

            let data = unsafe { da.write(0, len)?.unwrap() };
            data.copy_from_slice(msg);

            let read = da.read(0, len).unwrap();

            assert_eq!(read, msg);

            Ok(())
        })
    }

    #[test]
    fn trivial() -> io::Result<()> {
        with_temp_path(|path| {
            let _da = DiskBytesRaw::<1024>::open(&path)?;
            Ok(())
        })
    }
}
