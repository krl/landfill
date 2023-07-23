use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use parking_lot::Mutex;
use tempfile::TempDir;

struct LandfillInner {
    dir_path: PathBuf,
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
#[derive(Clone)]
pub struct Landfill(Arc<LandfillInner>);

impl Landfill {
    /// Opens a Landfill for further data dumping
    ///
    /// A directory is created if not already there, otherwise all prior
    /// data is ready to be re-requested
    pub fn open<P: AsRef<Path>>(dir_path: P) -> io::Result<Landfill> {
        let dir_path = dir_path.as_ref().into();
        fs::create_dir(&dir_path)?;

        Ok(Landfill(Arc::new(LandfillInner {
            _temp_dir: None,
            dir_path,
            self_destruct_sequence_initiated: Mutex::new(false),
        })))
    }

    /// Create a landfill backed by temporaray directories
    pub fn ephemeral() -> io::Result<Landfill> {
        let dir = tempfile::tempdir()?;
        let dir_path = dir.path().into();

        Ok(Landfill(Arc::new(LandfillInner {
            _temp_dir: Some(dir),
            dir_path,
            self_destruct_sequence_initiated: Mutex::new(false),
        })))
    }

    /// This function will remove all data written into this landfill as the last
    /// reference goes out of scope
    pub fn self_destruct(&self) {
        *self.0.self_destruct_sequence_initiated.lock() = true
    }
}

impl Drop for LandfillInner {
    fn drop(&mut self) {
        if *self.self_destruct_sequence_initiated.lock() {
            todo!()
        }
    }
}
