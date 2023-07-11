# landfill

The content-adressed bytestore you did not know that you wanted!

This database is built ground-up on append-only principles, this allows us to cut a lot of corners in the design, getting rid of any cache invalidation problems and similar complications. Values gotten from the database have a lifetime corresponding to the database itself, and it can be written to concurrently.

The database takes bytes and returns their hashes, which can then in turn be used to get backe references to the bytes. All data saved is cryptographically checked before given to the user, so data corruption will be detected.

## data layout

The data layout consists of 4 files

### header

The header stores the magic string "lnfl", a 32 bit version number (1, at the time of writing), and 4 keys used to key a non-cryptographic hash function/checksummer

### RawDiskBytes

This is a wrapper around a growable set of byte-chunks persisted on the disk.

It is implemented as an array of memory-mapped files, each double the size of the previous.

It has an unsafe `write` function, that could allow you to create aliasing mutable slices, therefore it should be used through one of the wrapper structs below

#### WriteOnceArray

This is a wrapper around `RawDiskBytes` that treats the bytes on disk as an array of type `T` values, where each value can be written to exactly once.

This way, the problem with aliasing goes away, since you can only request mutable access to uninitialized(all zero) slots.

#### JournaledData

This is a second wrapper around `RawDiskBytes` that keeps a `Journal` of already written bytes, so that you can only request a mutable slice at the end of the uninitialized memory.

The journal keeps a checksummed set of values indicating how many bytes have already been written, and is robust to crashes.

### index

The index keeps track of where data has been written, and is a variant of a	non-resizing hash map.

As data is inserted, it is first hashed with a cryptographic hash function (blake3 is default, but it is configurable via the Digest trait), this hash is in turn checksummed. This checksum is keyed by random factors and cannot be predicted by an attacker, similar to how rusts default hashmaps use randomness to avoid DOS attacks.

You can think of the datastructure as an array of hashmaps, each one doubling in size from the last. When you want to insert a key, you just look in its hash slot, and if it is already occupied, go on to the next one.

This way we never run out of space in the hashmap, with the tradeoff that we get `O(log n)` lookups instead of the `O(1)` of traditional hashmaps.

## TODO

assure that the memory growing functions are thread-safe, at the moment they do not use any locks
