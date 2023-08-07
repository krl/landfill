use std::io;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;

use bytemuck::{Pod, Zeroable};
use parking_lot::{RwLock, RwLockReadGuard};

use super::bytes::DiskBytes;
use crate::helpers;
use crate::{GuardedLandfill, Substructure};

const N_LOCKS: usize = 256;

/// An unbounded array of `T` on disk
///
/// Note that `T` must implement `Pod` and `Zeroable` and that additionally
/// the value of `Self::zeroed()`, i.e a representation consisting of all zeroes,
/// will be considered `None` for purpouses of accessing uninitialized elements of
/// the array
pub struct RandomAccess<T> {
    bytes: DiskBytes,
    locks: [RwLock<()>; N_LOCKS],
    _marker: PhantomData<T>,
}

pub struct RandomAccessGuard<'a, T> {
    item: &'a T,
    _guard: RwLockReadGuard<'a, ()>,
}

impl<'a, T> Deref for RandomAccessGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.item
    }
}

impl<T> Substructure for RandomAccess<T> {
    fn init(lf: GuardedLandfill) -> io::Result<Self> {
        let bytes = lf.substructure("array")?;

        const MUTEX: RwLock<()> = RwLock::new(());
        let locks = [MUTEX; N_LOCKS];

        Ok(RandomAccess {
            bytes,
            locks,
            _marker: PhantomData,
        })
    }

    fn flush(&self) -> io::Result<()> {
        self.bytes.flush()
    }
}

impl<T> RandomAccess<T>
where
    T: Zeroable + Pod,
{
    /// Get a reference to an element in the array
    ///
    /// Returns None if the element is uninitialized
    /// or equal to `Zeroable::zeroed()`.
    pub fn get(&self, index: usize) -> Option<RandomAccessGuard<T>> {
        let t_size = mem::size_of::<T>();
        let byte_offset = (index * t_size) as u64;

        let guard = self.locks[index % N_LOCKS].read();

        if let Some(slice) = self.bytes.read(byte_offset, t_size as u32) {
            let cast: &[T] = bytemuck::cast_slice(slice);
            debug_assert_eq!(cast.len(), 1);
            if !helpers::is_all_zeroes(cast) {
                Some(RandomAccessGuard {
                    item: &cast[0],
                    _guard: guard,
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Run a closure with mutable access to an element of the array
    ///
    /// Will grow the array as neccesary to be able to index the position
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

        // just to be explicit, it's not neccesary to manually drop this
        drop(guard);

        Ok(res)
    }
}
