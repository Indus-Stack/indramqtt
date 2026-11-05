`base64_encode(v)` base64-encodes `v`.

The call takes one to three arguments. The first is the value to encode.
It returns base64 text using the standard alphabet with padding. Extra
arguments are options: `urlsafe` and `no_padding`. A null value reads as
null. A wrong argument count reads as null.
