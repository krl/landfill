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

On opening a `Journal`, the largest value with a valid checksum is recovered, guarding against broken half-finished writes.

# RandomAccess

A random-access, automatically growing array of type `T`.

It works with a finite set of RWLocks, that are mapped to the index positions.

Reads take a readlock, and return guards, whereas writes can only happen in closures passed into the `with_mut` function. This is to avoid the possibility of deadlocking when trying to hold multiple mutable references at once.

Note that values stored consisting of all zeroes will be considered empty space, and return `None` on `get`.

# AppendOnly

An append only virtual file of bytes, it keeps an internal journal on how many bytes have already been written, and the data written _never moves_ in memory, so it can safely hand out references that live as long as the struct itself.

# OnceMap

A K-V map that maps keys to values, the values cannot be removed or updated, and thus it is safe to keep references to them even as more kv-pairs are added.

# Content

A store for content-addressed data, bytes written to this store will be hashed with the provided generic cryptographic hash-function, and a `ContentId` will be returned, that can in turn be used to again get a reference to the data.

This is similar to `OnceMap` in implementation, but saves on space since the key does not have to be stored, and is re-computed on fetch, giving an additional layer of protection protection against corrupted reads.
