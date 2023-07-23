use std::{
    cell::UnsafeCell,
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

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
    already_mapped: Mutex<HashSet<String>>,
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

        Ok(Landfill {
            inner: Arc::new(LandfillInner {
                _temp_dir: None,
                dir_path,
                self_destruct_sequence_initiated: Mutex::new(false),
                already_mapped: Mutex::new(HashSet::new()),
            }),
            name_prefix: String::new(),
        })
    }

    /// Create a landfill backed by temporaray directories
    pub fn ephemeral() -> io::Result<Landfill> {
        let dir = tempfile::tempdir()?;
        let dir_path = dir.path().into();

        Ok(Landfill {
            inner: Arc::new(LandfillInner {
                _temp_dir: Some(dir),
                dir_path,
                self_destruct_sequence_initiated: Mutex::new(false),
                already_mapped: Mutex::new(HashSet::new()),
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

    fn map_file_inner(
        &self,
        name: &str,
        size: u64,
        create: bool,
    ) -> io::Result<Option<MappedFile>> {
        println!("map file inner");
        println!("{name} {size} {create}");
        println!("{self:?}");

        let full_name = join_names(&self.name_prefix, name);
        let mut already_mapped = self.inner.already_mapped.lock();

        println!("full {full_name:?}");

        if already_mapped.get(&full_name).is_some() {
            println!("full name {full_name}");
            return Ok(None);
        }

        let mut full_path = PathBuf::from(&self.inner.dir_path);
        full_path.push(&full_name);

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
        already_mapped.insert(full_name.clone());

        Ok(Some(MappedFile { _file: file, map }))
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
    }
}

/// A file with a corresponding memory map of the entire contents of the file
pub struct MappedFile {
    _file: File,
    map: UnsafeCell<MmapMut>,
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
