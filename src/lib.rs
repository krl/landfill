//! Landfill
//!
//! A set of datastructures for dealing with persistent, on-disk data.

#![deny(missing_docs)]
// falsely triggers on `JournalEntry<T>`
#![allow(clippy::extra_unused_type_parameters)]
// to allow initialization of static arrays of Locks
#![allow(clippy::declare_interior_mutable_const)]

mod storage;
pub use storage::*;

mod structures;
pub use structures::*;

mod disk;
pub use disk::{Landfill, MappedFile};
