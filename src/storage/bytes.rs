use std::cell::UnsafeCell;
use std::fs::{File, OpenOptions};
use std::io;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use memmap2::MmapMut;
use parking_lot::Mutex;

const N_LANES: usize = 32;

struct Lane {
    #[allow(unused)]
    file: Option<File>,
    map: UnsafeCell<MmapMut>,
}

impl Lane {
    fn anon(size: u64) -> io::Result<Self> {
        let map = UnsafeCell::new(MmapMut::map_anon(size as usize)?);
        Ok(Lane { file: None, map })
    }

    fn disk<P: AsRef<Path>>(path: P, size: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(path.as_ref())?;

        file.set_len(size)?;
        let map = UnsafeCell::new(unsafe { MmapMut::map_mut(&file)? });
        Ok(Lane {
            file: Some(file),
            map,
        })
    }

    fn bytes(&self) -> &[u8] {
        unsafe { &*self.map.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn bytes_mut(&self) -> &mut [u8] {
        unsafe { &mut *self.map.get() }
    }

    fn flush(&self) -> io::Result<()> {
        if self.file.is_some() {
            unsafe { (*self.map.get()).flush() }
        } else {
            // We don't need to flush anon memory maps
            Ok(())
        }
    }
}

pub(crate) struct DiskBytes<const INIT_SIZE: u64> {
    root_path: Option<PathBuf>,
    lanes: [OnceLock<Lane>; N_LANES],
    io_mutex: Mutex<()>,
}

impl<const INIT_SIZE: u64> DiskBytes<INIT_SIZE> {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        const LOCK: OnceLock<Lane> = OnceLock::new();
        let lanes = [LOCK; N_LANES];
        let pb = PathBuf::from(path.as_ref());

        for (i, lane) in lanes.iter().enumerate() {
            let mut data_path = pb.clone();
            data_path.push(format!("{:02x}", i));

            if data_path.exists() {
                // `OnceLock::set` returns the value you tried to set, had it
                // already been initialized
                //
                // This is however always the first time this `OnceLock` is touched,
                // due to being created just above, thus this will never error.
                if lane
                    .set(Lane::disk(data_path, Self::lane_size(i))?)
                    .is_err()
                {
                    unreachable!()
                }
            }
        }

        Ok(DiskBytes {
            root_path: Some(pb),
            lanes,
            io_mutex: Mutex::new(()),
        })
    }

    pub fn ephemeral() -> io::Result<Self> {
        const LOCK: OnceLock<Lane> = OnceLock::new();
        let lanes = [LOCK; N_LANES];

        Ok(DiskBytes {
            root_path: None,
            lanes,
            io_mutex: Mutex::new(()),
        })
    }

    pub fn flush(&self) -> io::Result<()> {
        for lane in &self.lanes {
            if let Some(lane) = lane.get() {
                lane.flush()?
            }
        }

        Ok(())
    }

    pub fn find_space_for(offset: u64, len: usize) -> u64 {
        let (lane_nr, inner_offset) = Self::lane_nr_and_ofs(offset);
        let lane_size = Self::lane_size(lane_nr);
        if inner_offset + len as u64 <= lane_size {
            offset
        } else {
            // tail-recurse
            Self::find_space_for(offset + (lane_size - inner_offset), len)
        }
    }

    pub unsafe fn request_write(
        &self,
        offset: u64,
        len: usize,
    ) -> io::Result<&mut [u8]> {
        let (lane_nr, offset) = Self::lane_nr_and_ofs(offset);
        let lane_size = Self::lane_size(lane_nr);

        if offset + len as u64 > lane_size {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Cannot write between lanes",
            ))
        } else {
            let mut lane_initialized = self.lanes[lane_nr].get();

            // Make sure the lane is initialized
            if lane_initialized.is_none() {
                let _guard = self.io_mutex.lock();
                lane_initialized = self.lanes[lane_nr].get();
                // After io_mutex is taken, we need to check again
                // that no other thread has come before us to initialize the
                // lane
                if lane_initialized.is_none() {
                    let lane = if let Some(root_path) = &self.root_path {
                        let mut data_path = root_path.clone();
                        data_path.push(format!("{:02x}", lane_nr));
                        Lane::disk(&data_path, lane_size)?
                    } else {
                        Lane::anon(lane_size)?
                    };
                    // Again, this error should never trigger since we have locked
                    // our io_mutex in this thread specifically
                    let _ = self.lanes[lane_nr].set(lane);
                    lane_initialized =
                        Some(self.lanes[lane_nr].get().expect("Just set above"))
                }
            }

            let lane_initialized = lane_initialized
                .expect("Above logic will always assure an initialized lane");

            return Ok(
                &mut lane_initialized.bytes_mut()[offset as usize..][..len]
            );
        }
    }

    pub fn read(&self, offset: u64, len: u32) -> Option<&[u8]> {
        let (lane, offset) = Self::lane_nr_and_ofs(offset);
        let lane_size = Self::lane_size(lane);

        if offset + len as u64 > lane_size {
            // We cannot read in lane boundaries
            None
        } else if let Some(lane) = self.lanes[lane].get() {
            let lane_bytes = lane.bytes();
            Some(&lane_bytes[offset as usize..offset as usize + len as usize])
        } else {
            None
        }
    }

    #[cfg(test)]
    fn lane_nr_and_ofs_slow_but_obviously_correct(
        mut offset: u64,
    ) -> (usize, u64) {
        let mut lane_nr = 0;

        loop {
            let lane_size = Self::lane_size(lane_nr);
            if lane_size <= offset {
                lane_nr += 1;
                offset -= lane_size;
            } else {
                return (lane_nr, offset);
            }
        }
    }

    fn lane_nr_and_ofs(offset: u64) -> (usize, u64) {
        let usize_bits = mem::size_of::<usize>() * 8;
        let i = offset / INIT_SIZE + 1;
        let lane_nr = usize_bits - i.leading_zeros() as usize - 1;
        let offset = offset - (2u64.pow(lane_nr as u32) - 1) * INIT_SIZE;
        (lane_nr, offset)
    }

    fn lane_size(lane: usize) -> u64 {
        INIT_SIZE * 2u64.pow(lane as u32)
    }
}

unsafe impl<const INIT_SIZE: u64> Send for DiskBytes<INIT_SIZE> {}
unsafe impl<const INIT_SIZE: u64> Sync for DiskBytes<INIT_SIZE> {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_lane_math_trivial() {
        assert_eq!(DiskBytes::<32>::lane_nr_and_ofs(0), (0, 0));
        assert_eq!(DiskBytes::<32>::lane_nr_and_ofs(31), (0, 31));
        assert_eq!(DiskBytes::<32>::lane_nr_and_ofs(32), (1, 0));
        assert_eq!(DiskBytes::<32>::lane_nr_and_ofs(64), (1, 32));
        assert_eq!(DiskBytes::<32>::lane_nr_and_ofs(95), (1, 63));
        assert_eq!(DiskBytes::<32>::lane_nr_and_ofs(96), (2, 0));
    }

    #[test]
    fn test_lane_math() {
        for i in 0..1024 * 256 {
            assert_eq!(
                DiskBytes::<32>::lane_nr_and_ofs(i),
                DiskBytes::<32>::lane_nr_and_ofs_slow_but_obviously_correct(i),
            );

            assert_eq!(
                DiskBytes::<1>::lane_nr_and_ofs(i),
                DiskBytes::<1>::lane_nr_and_ofs_slow_but_obviously_correct(i),
            );

            assert_eq!(
                DiskBytes::<1024>::lane_nr_and_ofs(i),
                DiskBytes::<1024>::lane_nr_and_ofs_slow_but_obviously_correct(
                    i
                ),
            );

            assert_eq!(
                DiskBytes::<17>::lane_nr_and_ofs(i),
                DiskBytes::<17>::lane_nr_and_ofs_slow_but_obviously_correct(i),
            );
        }
    }

    #[test]
    fn test_lane_sizes() {
        assert_eq!(DiskBytes::<32>::lane_size(0), 32);
        assert_eq!(DiskBytes::<32>::lane_size(1), 64);
        assert_eq!(DiskBytes::<32>::lane_size(2), 128);

        assert_eq!(DiskBytes::<1024>::lane_size(0), 1024);
        assert_eq!(DiskBytes::<1024>::lane_size(1), 2048);
        assert_eq!(DiskBytes::<1024>::lane_size(2), 4096);
    }

    #[test]
    fn simple_write_read() -> io::Result<()> {
        let db = DiskBytes::<1024>::ephemeral()?;

        let msg = b"hello world";
        let len = msg.len();

        unsafe { db.request_write(0, len)? }.copy_from_slice(msg);

        let read = db.read(0, len as u32).unwrap();

        assert_eq!(read, msg);

        Ok(())
    }

    #[test]
    fn find_space() -> io::Result<()> {
        assert_eq!(DiskBytes::<1>::find_space_for(0, 0), 0);
        assert_eq!(DiskBytes::<1>::find_space_for(0, 1), 0);
        assert_eq!(DiskBytes::<1>::find_space_for(1, 1), 1);

        assert_eq!(DiskBytes::<1>::find_space_for(2, 1), 2);
        assert_eq!(DiskBytes::<1>::find_space_for(2, 2), 3);

        assert_eq!(DiskBytes::<1>::find_space_for(100, 100), 127);
        assert_eq!(DiskBytes::<1>::find_space_for(0, 100), 127);

        Ok(())
    }
}
