use std::io;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;

use bytemuck::{Pod, Zeroable};
use parking_lot::{RwLock, RwLockReadGuard};

use super::bytes::DiskBytes;
use crate::Landfill;

const N_LOCKS: usize = 256;

/// An unbounded array of `T` on disk
///
/// Note that `T` must implement `Pod` and `Zeroable` and that additionally
/// the value of `Self::zeroed()`, i.e a representation consisting of all zeroes,
/// will be considered `None` for purpouses of accessing uninitialized elements of
/// the array
pub struct Array<T> {
    bytes: DiskBytes,
    locks: [RwLock<()>; N_LOCKS],
    _marker: PhantomData<T>,
}

pub struct ArrayGuard<'a, T> {
    item: &'a T,
    _guard: RwLockReadGuard<'a, ()>,
}

impl<'a, T> Deref for ArrayGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.item
    }
}

impl<T> TryFrom<&Landfill> for Array<T> {
    type Error = io::Error;

    /// Opens a new array at specified path, creating a directory if neccesary
    fn try_from(landfill: &Landfill) -> io::Result<Self> {
        let landfill = landfill.branch("array");
        let bytes = DiskBytes::try_from(&landfill)?;

        const MUTEX: RwLock<()> = RwLock::new(());
        let locks = [MUTEX; N_LOCKS];

        Ok(Array {
            bytes,
            locks,
            _marker: PhantomData,
        })
    }
}

impl<T> Array<T>
where
    T: Zeroable + Pod + PartialEq,
{
    /// Flush the in-memory changes to disk
    ///
    /// This call is blocking until the writes are complete
    pub fn flush(&self) -> io::Result<()> {
        self.bytes.flush()
    }

    /// Get a reference to an element in the array
    ///
    /// Returns None if the element is uninitialized
    /// or equal to `Zeroable::zeroed()`.
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

        // not neccesary to manually drop this,
        // we're explicit just to be clear that it's over.
        drop(guard);

        Ok(res)
    }
}
