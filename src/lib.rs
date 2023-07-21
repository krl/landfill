//! Landfill
//!
//! A set of datastructures for dealing with persistent, on-disk data.

#![deny(missing_docs)]
// falsely triggers on `JournalEntry<T>`
#![allow(clippy::extra_unused_type_parameters)]
// to allow initialization of static arrays of Locks
#![allow(clippy::declare_interior_mutable_const)]

mod appendonly;
mod array;
mod bytes;
mod entropy;
mod journal;

pub use appendonly::AppendOnly;
pub use array::Array;
pub use entropy::Entropy;
pub use journal::Journal;
