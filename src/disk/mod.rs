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
use tempfile::TempDir;

fn join_names(name1: &str, name2: &str) -> String {
    if name1.len() == 0 {
        name2.into()
    } else {
        format!("{name1}_{name2}")
    }
}

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
    dir_path: PathBuf,
    reserved_paths: Mutex<HashSet<PathBuf>>,
    self_destruct_sequence_initiated: Mutex<bool>,

    // to manage the lifetime of temporary directories
    _temp_dir: Option<TempDir>,
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
                _temp_dir: None,
                dir_path,
                self_destruct_sequence_initiated: Mutex::new(false),
                reserved_paths: Mutex::new(HashSet::new()),
            }),
            name_prefix: String::new(),
        })
    }

    /// Create a landfill backed by temporaray directories
    pub fn ephemeral() -> io::Result<Landfill> {
        let dir = tempfile::tempdir()?;
        let dir_path: PathBuf = dir.path().into();

        Ok(Landfill {
            inner: Arc::new(LandfillInner {
                _temp_dir: Some(dir),
                dir_path,
                self_destruct_sequence_initiated: Mutex::new(false),
                reserved_paths: Mutex::new(HashSet::new()),
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

        if !self.register_path(branch.active_path()) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Attempt at mapping the same substructure twice",
            ));
        }

        let guarded = GuardedLandfill { guarded: branch };

        S::init(guarded)
    }

    fn branch(&self, name: String) -> Self {
        let new_name = join_names(&self.name_prefix, &name);

        Landfill {
            inner: self.inner.clone(),
            name_prefix: new_name,
        }
    }

    /// Mark this landfill for destruction
    ///
    /// Data will be deleted as soon as the last reference to this landfill
    /// goes out of scope
    pub fn initiate_self_destruct_sequence(&self) {
        *self.inner.self_destruct_sequence_initiated.lock() = true;
    }

    fn active_path(&self) -> PathBuf {
        let mut path = self.inner.dir_path.clone();
        path.push(&self.name_prefix);
        path
    }

    fn active_path_plus(&self, name: &str) -> PathBuf {
        let mut path = self.inner.dir_path.clone();
        path.push(format!("{}_{}", &self.name_prefix, name));
        path
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
        let path = self.active_path();

        let t = if path.exists() {
            let t = T::zeroed();
            let t_slice = &mut [t];
            let byte_slice: &mut [u8] = bytemuck::cast_slice_mut(t_slice);

            let mut file = OpenOptions::new().read(true).open(&path)?;

            file.read_exact(byte_slice)?;
            t
        } else {
            let t = init();
            let t_slice = &[t];
            let byte_slice: &[u8] = bytemuck::cast_slice(t_slice);

            let mut file =
                OpenOptions::new().write(true).create(true).open(&path)?;

            file.write_all(byte_slice)?;
            file.flush()?;
            t
        };
        Ok(t)
    }

    fn register_path(&self, path: PathBuf) -> bool {
        self.inner.reserved_paths.lock().insert(path)
    }

    /// Open a file mapping, creating a file if none previously existed
    ///
    /// Returns `None` if the file has already been mapped
    pub fn map_file_create(
        &self,
        name: String,
        size: u64,
    ) -> io::Result<Option<MappedFile>> {
        let path = self.active_path_plus(&name);

        if self.register_path(path.clone()) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;

            file.set_len(size)?;

            let map = UnsafeCell::new(unsafe { MmapMut::map_mut(&file)? });

            Ok(Some(MappedFile {
                _file: file,
                map,
                _fill: self.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Open a file map, if it already exists
    pub fn map_file_existing(
        &self,
        name: String,
        size: u64,
    ) -> io::Result<Option<MappedFile>> {
        let path = self.active_path_plus(&name);

        if path.exists() {
            if self.register_path(path.clone()) {
                match OpenOptions::new().read(true).write(true).open(&path) {
                    Ok(file) => {
                        file.set_len(size)?;
                        let map = UnsafeCell::new(unsafe {
                            MmapMut::map_mut(&file)?
                        });
                        Ok(Some(MappedFile {
                            _file: file,
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
        if self._temp_dir.is_none() {
            // non-volatile paths comes with with lockfiles
            let mut lock_file_path = self.dir_path.clone();
            lock_file_path.push("_lock");
            let _ = fs::remove_file(lock_file_path);
        }
        if *self.self_destruct_sequence_initiated.lock() {
            let _ = fs::remove_dir_all(&self.dir_path);
        }
    }
}

/// A file with a corresponding memory map of the entire contents of the file
pub struct MappedFile {
    map: UnsafeCell<MmapMut>,
    _file: File,
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
    /// Highly unsafe! You must guarantee that this only happens from one thread
    /// at once.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn bytes_mut(&self) -> &mut [u8] {
        unsafe { &mut *self.map.get() }
    }

    /// Flushes the file to the backing disk, blocks until done
    pub fn flush(&self) -> io::Result<()> {
        unsafe { (*self.map.get()).flush() }
    }
}
