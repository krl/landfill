# landfill

The content-adressed bytestore you did not know that you needed!

## data layout

  [ 4 | magic bytes = b"lnfl" ]
  [ 4 | version_nr ]

  [ 8 | occupied data ]
  [ 8 | occupied data checksum ]
  [ 8 | occupied data copy ]
  [ 8 | occupied data checksum copy]

[ padding until the first tree node]

A tree node is made up of 256 slots, each slot has the following layout,

Since each slot is 32 bytes,

  [ 8  | data offset ]
  [ 4  | data len ]
  [ 12 | key discriminant ]
  [ 8  | next_node ]
