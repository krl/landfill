use std::{
    cell::UnsafeCell,
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytemuck::{Pod, Zeroable};
use memmap2::MmapMut;
use parking_lot::Mutex;

/// A guard around a landfill that can only be created from this module
pub struct GuardedLandfill {
    guarded: Landfill,
}

impl GuardedLandfill {
    /// Returns the wrapped `Landfill`, consuming the guard
    pub fn inner(self) -> Landfill {
        self.guarded
    }
}

impl Deref for GuardedLandfill {
    type Target = Landfill;
    fn deref(&self) -> &Landfill {
        &self.guarded
    }
}

/// A datastructure that can be constructed from a landfill
pub trait Substructure: Sized {
    /// Initialize a datastructure of this type, backed by `landfill`
    fn init(landfill: GuardedLandfill) -> io::Result<Self>;
    /// Flush all data to disk
    fn flush(&self) -> io::Result<()>;
}

#[derive(Debug)]
struct LandfillInner {
    dir_path: Option<PathBuf>,
    reserved_names: Mutex<HashSet<String>>,
    self_destruct_sequence_initiated: Mutex<bool>,
}

/// The datastructure representing an on-disk data dump
///
/// This abstraction corresponds to a directory in the filesystem
/// which can host files for multiple datastructures
///
/// The data in a `Landfill` should be considered siloed, it contains both
/// secret nonces that could be used to DOS hash-maps if known, and machine-specific
/// endian data. Not for sharing.
///
/// It is also strictly growing, and only supports one delete operation, the self
/// destruct sequence, that removes all data associated with itself when triggered.
#[derive(Clone, Debug)]
pub struct Landfill {
    inner: Arc<LandfillInner>,
    name_prefix: String,
}

impl Landfill {
    /// Opens a Landfill for further data dumping
    ///
    /// A directory is created if not already there, otherwise all prior
    /// data is ready to be re-requested
    pub fn open<P: AsRef<Path>>(dir_path: P) -> io::Result<Landfill> {
        let dir_path: PathBuf = dir_path.as_ref().into();
        if !dir_path.exists() {
            fs::create_dir(&dir_path)?;
        }

        let mut lock_file_path = dir_path.clone();
        lock_file_path.push("_lock");

        // aquire filesystem lock
        let _lock = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_file_path)?;

        Ok(Landfill {
            inner: Arc::new(LandfillInner {
                dir_path: Some(dir_path),
                self_destruct_sequence_initiated: Mutex::new(false),
                reserved_names: Mutex::new(HashSet::new()),
            }),
            name_prefix: String::new(),
        })
    }

    /// Create a landfill backed by temporaray directories
    pub fn ephemeral() -> io::Result<Landfill> {
        Ok(Landfill {
            inner: Arc::new(LandfillInner {
                dir_path: None,
                self_destruct_sequence_initiated: Mutex::new(false),
                reserved_names: Mutex::new(HashSet::new()),
            }),
            name_prefix: String::new(),
        })
    }

    /// Create a substructure of type `S` with name `N` in the landfill
    pub fn substructure<S, N>(&self, name: N) -> io::Result<S>
    where
        S: Substructure,
        N: Into<String>,
    {
        let branch = self.branch(name.into());

        if !self.register_name(branch.full_name()) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Attempt at mapping the same substructure twice",
            ));
        }

        let guarded = GuardedLandfill { guarded: branch };

        S::init(guarded)
    }

    pub(crate) fn branch(&self, mut name: String) -> Self {
        if !self.name_prefix.is_empty() {
            name = format!("{}_{name}", self.name_prefix);
        }

        Landfill {
            inner: self.inner.clone(),
            name_prefix: name,
        }
    }

    /// Mark this landfill for destruction
    ///
    /// Data will be deleted as soon as the last reference to this landfill
    /// goes out of scope
    pub fn initiate_self_destruct_sequence(&self) {
        *self.inner.self_destruct_sequence_initiated.lock() = true;
    }

    fn full_name(&self) -> String {
        self.name_prefix.clone()
    }

    fn active_path(&self) -> Option<PathBuf> {
        self.inner.dir_path.as_ref().map(|path| {
            let name = self.full_name();
            let mut path = path.clone();
            path.push(name);
            path
        })
    }

    /// Reads a static file into type `T` if it exists
    ///
    /// Otherwise it calls the `init` closure to create and write a new
    /// file containing the result.
    pub fn get_static_or_init<Init, T>(&self, init: Init) -> io::Result<T>
    where
        Init: Fn() -> T,
        T: Zeroable + Pod,
    {
        if let Some(path) = self.active_path() {
            if path.exists() {
                let t = T::zeroed();
                let t_slice = &mut [t];
                let byte_slice: &mut [u8] = bytemuck::cast_slice_mut(t_slice);

                let mut file = OpenOptions::new().read(true).open(&path)?;

                file.read_exact(byte_slice)?;
                Ok(t)
            } else {
                let t = init();
                let t_slice = &[t];
                let byte_slice: &[u8] = bytemuck::cast_slice(t_slice);

                let mut file =
                    OpenOptions::new().write(true).create(true).open(&path)?;

                file.write_all(byte_slice)?;
                file.flush()?;
                Ok(t)
            }
        } else {
            // ephemeral landfill, no io necessary
            Ok(init())
        }
    }

    fn register_name(&self, name: String) -> bool {
        let mut names = self.inner.reserved_names.lock();

        names.insert(name)
    }

    /// Open a file mapping, creating a file if none previously existed
    ///
    /// Returns `None` if the file has already been mapped
    pub fn map_file_create(&self, size: u64) -> io::Result<Option<MappedFile>> {
        if !self.register_name(self.full_name()) {
            if let Some(path) = self.active_path() {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)?;

                file.set_len(size)?;

                let map = UnsafeCell::new(unsafe { MmapMut::map_mut(&file)? });

                Ok(Some(MappedFile {
                    _file: Some(file),
                    map,
                    _fill: self.clone(),
                }))
            } else {
                let map = UnsafeCell::new(MmapMut::map_anon(size as usize)?);

                Ok(Some(MappedFile {
                    _file: None,
                    map,
                    _fill: self.clone(),
                }))
            }
        } else {
            // Already registered
            Ok(None)
        }
    }

    /// Open a file map, if it already exists
    pub fn map_file_existing(
        &self,
        size: u64,
    ) -> io::Result<Option<MappedFile>> {
        let full_name = self.full_name();
        if !self.register_name(full_name) {
            return Ok(None);
        }

        if let Some(path) = self.active_path() {
            if path.exists() {
                match OpenOptions::new().read(true).write(true).open(&path) {
                    Ok(file) => {
                        file.set_len(size)?;
                        let map = UnsafeCell::new(unsafe {
                            MmapMut::map_mut(&file)?
                        });
                        Ok(Some(MappedFile {
                            _file: Some(file),
                            map,
                            _fill: self.clone(),
                        }))
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e),
                }
            } else {
                Ok(None)
            }
        } else {
            // Ephemeral, no maps exist alreay
            Ok(None)
        }
    }

    /// This function will remove all data written into this landfill as the last
    /// reference goes out of scope
    pub fn self_destruct(&self) {
        *self.inner.self_destruct_sequence_initiated.lock() = true
    }
}

impl Drop for LandfillInner {
    fn drop(&mut self) {
        if let Some(dir_path) = self.dir_path.as_ref() {
            // non-volatile paths comes with with lockfiles
            let mut lock_file_path = dir_path.clone();
            lock_file_path.push("_lock");
            let _ = fs::remove_file(lock_file_path);

            // remove all files if self destruct sequence was initiated
            if *self.self_destruct_sequence_initiated.lock() {
                let _ = fs::remove_dir_all(dir_path);
            }
        }
    }
}

/// A file with a corresponding memory map of the entire contents of the file
pub struct MappedFile {
    map: UnsafeCell<MmapMut>,
    _file: Option<File>,
    _fill: Landfill,
}

impl AsRef<[u8]> for MappedFile {
    fn as_ref(&self) -> &[u8] {
        unsafe { &(*self.map.get())[..] }
    }
}

impl MappedFile {
    /// Returns a mutable reference into the bytes of the mapped file
    ///
    /// # Safety
    /// You must manually guarantee that this slice never aliases
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn bytes_mut(&self) -> &mut [u8] {
        unsafe { &mut *self.map.get() }
    }

    /// Flushes the file to the backing disk, blocks until done
    pub fn flush(&self) -> io::Result<()> {
        unsafe { (*self.map.get()).flush() }
    }
}
