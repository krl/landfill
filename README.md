# landfill

A set of datastructures for dealing with persistent, on-disk data.

The goal is to have a persistance-layer that is easy to reason about, allow for concurrent access and mutation, and provide safe and powerful abstractions around data stored on disk.

The design of landfill leans heavily to the simplistic side, and leaves all caching and page handling to the kernel, via memory mapping.

In general the structures implemented here can _only grow_, if any sort of gargbage collection is needed, it would have to be implemented on a layer above this.

The library has 4 main components, that each correspond to files or sets of files.

# Entropy

The `Entropy` component is a file 256 bytes long that contain 4 random u64's.

This is used to do checksumming, create nonces, provide tags for distinguishing different stores from each other etc.

# Journal

The `Journal` stores a number of incremental updates, such as the writehead of a `AppendOnly` buffer, and is designed to be thread-safe and crash tolerant.

It works by saving multiple versions of the value, along with their corresponding checksums.

On `open`ing a `Journal`, the largest value with a valid checksum is recovered, guarding against broken half-finished writes.

# Array

The array can be thougth of as an automatically growing array of type `T`.

It works with a finite set of RWLocks (`N_LOCKS`), that are mapped to the index positions.

Reads take a readlock, and return guards, whereas writes can only happen in closures passed into the `with_mut` function. This is to avoid the possibility of deadlocking when trying to hold multiple mutable references at once.

# AppendOnly

An append only virtual file of bytes, it keeps an internal journal on how many bytes have already been written, and the data written _never moves_ in memory, so it can safely hand out &[u8] references that live as long as the struct itself.
