use std::io;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::path::Path;

use bytemuck::{Pod, Zeroable};
use parking_lot::{RwLock, RwLockReadGuard};

use crate::bytes::DiskBytes;

const N_LOCKS: usize = 256;

pub struct Array<T, const INIT_SIZE: u64> {
    bytes: DiskBytes<INIT_SIZE>,
    locks: [RwLock<()>; N_LOCKS],
    _marker: PhantomData<T>,
}

pub struct ArrayGuard<'a, T> {
    item: &'a T,
    #[allow(unused)]
    guard: RwLockReadGuard<'a, ()>,
}

impl<'a, T> Deref for ArrayGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.item
    }
}

impl<T, const INIT_SIZE: u64> Array<T, INIT_SIZE>
where
    T: Zeroable + Pod + PartialEq,
{
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let bytes = DiskBytes::open(path)?;

        const MUTEX: RwLock<()> = RwLock::new(());
        let locks = [MUTEX; N_LOCKS];

        Ok(Array {
            bytes,
            locks,
            _marker: PhantomData,
        })
    }

    pub fn ephemeral() -> io::Result<Self> {
        let bytes = DiskBytes::ephemeral()?;

        const MUTEX: RwLock<()> = RwLock::new(());
        let locks = [MUTEX; N_LOCKS];

        Ok(Array {
            bytes,
            locks,
            _marker: PhantomData,
        })
    }

    pub fn flush(&self) -> io::Result<()> {
        self.bytes.flush()
    }

    pub fn get(&self, index: usize) -> Option<ArrayGuard<T>> {
        let t_size = mem::size_of::<T>();
        let byte_offset = (index * t_size) as u64;

        let guard = self.locks[index % N_LOCKS].read();

        if let Some(slice) = self.bytes.read(byte_offset, t_size as u32) {
            let cast: &[T] = bytemuck::cast_slice(slice);
            debug_assert_eq!(cast.len(), 1);
            if cast[0] != T::zeroed() {
                Some(ArrayGuard {
                    item: &cast[0],
                    guard,
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn with_mut<F, R>(&self, index: usize, mut closure: F) -> io::Result<R>
    where
        F: FnMut(&mut T) -> R,
    {
        let t_size = mem::size_of::<T>();
        let byte_offset = (index * t_size) as u64;

        let guard = self.locks[index % N_LOCKS].write();

        let slice = unsafe { self.bytes.request_write(byte_offset, t_size)? };

        let t_slice = bytemuck::cast_slice_mut(slice);
        assert!(t_slice.len() == 1);
        let t = &mut t_slice[0];

        let res = closure(t);

        drop(guard);

        Ok(res)
    }
}
