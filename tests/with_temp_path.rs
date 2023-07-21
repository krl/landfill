use std::io;
use std::path::{Path, PathBuf};
use tempfile;

#[allow(unused)]
pub fn with_temp_path<R, F>(f: F) -> io::Result<R>
where
    F: Fn(&Path) -> io::Result<R>,
{
    let dir = tempfile::tempdir()?;
    let path = PathBuf::from(dir.path());
    f(path.as_ref())
}
