use std::io;
use std::mem;
use std::sync::OnceLock;

use crate::{Landfill, MappedFile};

const N_LANES: usize = 32;

pub(crate) struct DiskBytes<const INIT_SIZE: u64> {
    landfill: Landfill,
    lanes: [OnceLock<MappedFile>; N_LANES],
}

impl<const INIT_SIZE: u64> TryFrom<&Landfill> for DiskBytes<INIT_SIZE> {
    type Error = io::Error;

    fn try_from(landfill: &Landfill) -> Result<Self, Self::Error> {
        const LOCK: OnceLock<MappedFile> = OnceLock::new();
        let lanes = [LOCK; N_LANES];

        for (i, lane) in lanes.iter().enumerate() {
            if let Some(lane_file) =
                landfill.map_file(&format!("{:02x}", i), Self::lane_size(i))?
            {
                // `OnceLock::set` returns the value you tried to set, had it
                // already been initialized
                //
                // This is however always the first time this `OnceLock` is touched,
                // due to being created just above, thus this will never error.

                if lane.set(lane_file).is_err() {
                    unreachable!()
                }
            }
        }

        Ok(DiskBytes {
            landfill: landfill.clone(),
            lanes,
        })
    }
}

impl<const INIT_SIZE: u64> DiskBytes<INIT_SIZE> {
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
            while lane_initialized.is_none() {
                let name = format!("{:02x}", lane_nr);

                if let Some(lane_file) =
                    self.landfill.map_file_create(&name, lane_size)?
                {
                    // Since we got the file from the landfill, we can be sure
                    // that no other thread has been able to progress here
                    //
                    // Initializing here will thus always succeed, and we can ignore
                    // the `Result` of setting te once lock
                    let _ = self.lanes[lane_nr].set(lane_file);
                    lane_initialized =
                        Some(self.lanes[lane_nr].get().expect("Just set above"))
                } else {
                    // spin
                    lane_initialized = self.lanes[lane_nr].get();
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
            let lane_bytes = lane.as_ref();
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
        let landfill = Landfill::ephemeral()?;
        let db = DiskBytes::<1024>::try_from(&landfill)?;

        let msg = b"hello world";
        let len = msg.len();

        unsafe { db.request_write(0, len)? }.copy_from_slice(msg);

        println!("snuvan");

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
