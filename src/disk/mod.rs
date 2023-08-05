use std::{
    cell::UnsafeCell,
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
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

#[derive(Debug)]
struct LandfillInner {
    dir_path: PathBuf,
    already_mapped: Mutex<HashSet<PathBuf>>,
    self_destruct_sequence_initiated: Mutex<bool>,
    // lockfile to prevent multiple landfills using the same path at the same time
    lock_file_path: Option<PathBuf>,
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
                already_mapped: Mutex::new(HashSet::new()),
                lock_file_path: Some(lock_file_path),
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
                already_mapped: Mutex::new(HashSet::new()),
                lock_file_path: None,
            }),
            name_prefix: String::new(),
        })
    }

    /// Create a sub-namespace in the landfill
    ///
    /// This works by setting a filename prefix, and does not create any
    /// filesystem directories
    pub fn branch(&self, name: &str) -> Self {
        let new_name = join_names(&self.name_prefix, name);
        Landfill {
            inner: self.inner.clone(),
            name_prefix: new_name,
        }
    }

    fn full_path_for(&self, name: &str) -> PathBuf {
        let mut full_path = PathBuf::from(&self.inner.dir_path);
        full_path.push(&join_names(&self.name_prefix, name));
        full_path
    }

    fn map_file_inner(
        &self,
        name: &str,
        size: u64,
        create: bool,
    ) -> io::Result<Option<MappedFile>> {
        let full_path = self.full_path_for(name);

        let mut already_mapped = self.inner.already_mapped.lock();

        if already_mapped.get(&full_path).is_some() {
            return Ok(None);
        }

        let file = match (create, full_path.exists()) {
            (false, false) => return Ok(None),
            (true, false) => OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&full_path)?,
            (_, true) => OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(&full_path)?,
        };

        file.set_len(size)?;
        let map = UnsafeCell::new(unsafe { MmapMut::map_mut(&file)? });

        // Register that we have this file mapped
        already_mapped.insert(full_path);

        Ok(Some(MappedFile {
            _file: file,
            map,
            _fill: self.clone(),
        }))
    }

    /// Reads a static file into type `T` if it exists
    ///
    /// Otherwise it calls the `init` closure to create and write a new
    /// file containing the result.
    pub fn get_static_or_init<Init, T>(
        &self,
        name: &str,
        init: Init,
    ) -> io::Result<T>
    where
        Init: Fn() -> T,
        T: Zeroable + Pod,
    {
        let full_path = self.full_path_for(name);

        if full_path.exists() {
            let t = T::zeroed();
            let t_slice = &mut [t];
            let byte_slice: &mut [u8] = bytemuck::cast_slice_mut(t_slice);

            let mut file = OpenOptions::new().read(true).open(&full_path)?;

            file.read_exact(byte_slice)?;

            Ok(t)
        } else {
            let t = init();
            let t_slice = &[t];
            let byte_slice: &[u8] = bytemuck::cast_slice(t_slice);

            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&full_path)?;

            file.write_all(byte_slice)?;
            file.flush()?;
            Ok(t)
        }
    }

    /// Open a file mapping, creating a file if none previously existed
    pub fn map_file_create(
        &self,
        name: &str,
        size: u64,
    ) -> io::Result<Option<MappedFile>> {
        self.map_file_inner(name, size, true)
    }

    /// Open a file map, if it is already existing
    pub fn map_file(
        &self,
        name: &str,
        size: u64,
    ) -> io::Result<Option<MappedFile>> {
        self.map_file_inner(name, size, false)
    }

    /// This function will remove all data written into this landfill as the last
    /// reference goes out of scope
    pub fn self_destruct(&self) {
        *self.inner.self_destruct_sequence_initiated.lock() = true
    }
}

impl Drop for LandfillInner {
    fn drop(&mut self) {
        if *self.self_destruct_sequence_initiated.lock() {
            todo!()
        }
        if let Some(lock_path) = self.lock_file_path.take() {
            let _ = fs::remove_file(lock_path);
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
