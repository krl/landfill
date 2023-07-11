use std::io;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use arr_macro::arr;
use bytemuck::{Pod, Zeroable};
use parking_lot::{Mutex, MutexGuard};

use crate::diskbytes::raw::DiskBytesRaw;

const N_LOCKS: usize = 256;

pub struct WriteOnceArray<const INIT_SIZE: usize, T> {
    raw: DiskBytesRaw<INIT_SIZE>,
    muticies: [Mutex<()>; N_LOCKS],
    _marker: PhantomData<T>,
}

impl<const INIT_SIZE: usize, T> WriteOnceArray<INIT_SIZE, T>
where
    T: Zeroable + Pod + PartialEq + Eq,
{
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let muticies = arr![Mutex::new(()); 256];
        let raw = DiskBytesRaw::open(path)?;

        Ok(WriteOnceArray {
            raw,
            muticies,
            _marker: PhantomData,
        })
    }

    pub fn get_nonzero(&self, index: usize) -> Option<&T> {
        let t_size = mem::size_of::<T>();
        let byte_offset = index * t_size;
        if let Some(slice) = self.raw.read(byte_offset, t_size) {
            let cast: &[T] = bytemuck::cast_slice(slice);
            debug_assert_eq!(cast.len(), 1);
            if cast[0] != T::zeroed() {
                Some(&cast[0])
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn initialize(
        &self,
        index: usize,
    ) -> io::Result<Option<Initialize<T>>> {
        let t_size = mem::size_of::<T>();
        let byte_offset = index * t_size;

        let guard = self.muticies[index % N_LOCKS].lock();

        if let Some(slice) = unsafe { self.raw.write(byte_offset, t_size)? } {
            let t_slice = bytemuck::cast_slice_mut(slice);
            assert!(t_slice.len() == 1);

            let uninitialized = &mut t_slice[0];

            if *uninitialized != T::zeroed() {
                Ok(None)
            } else {
                Ok(Some(Initialize {
                    item: uninitialized,
                    guard,
                }))
            }
        } else {
            Ok(None)
        }
    }
}

pub struct Initialize<'a, T> {
    item: &'a mut T,
    #[allow(unused)]
    guard: MutexGuard<'a, ()>,
}

impl<'a, T> Deref for Initialize<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

impl<'a, T> DerefMut for Initialize<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.item
    }
}
