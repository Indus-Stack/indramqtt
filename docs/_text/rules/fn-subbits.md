`subbits(<b>, <len>)` returns the first `len` bits of the string `b` as an integer.
`subbits(<b>, <start>, <len>)` starts at the 1-based bit position `start`.
The full form is `subbits(<b>, <start>, <len>, <type>, <signedness>, <endianness>)`.

The first argument must be a string. Bits run most significant first
inside each byte. The call accepts two to six arguments.

The default type is `integer`, the default signedness is `unsigned`,
and the default endianness is `big`. The type accepts `integer`,
`float`, and `bits`; `binary` is an alias of `bits`. The signedness
accepts `signed` and `unsigned`. The endianness accepts `big` and
`little`. A value outside these sets reads as null.

The length clamps to the bits that remain. A start position one past
the last bit reads as 0 when the type is `integer`; a start position
further out reads as null. An integer or float slice wider than 64
bits reads as null. A `float` slice that is not 32 or 64 bits wide
reads as null. A `bits` slice returns a base64 string of the covered
bytes. A wrong argument count reads as null.
