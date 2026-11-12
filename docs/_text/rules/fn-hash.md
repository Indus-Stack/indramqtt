`hash(<algo>, <data>)` returns the hex digest of `data`.

The call takes two arguments. The first names the algorithm. The
second holds the text. The name ignores case and edge space.
Supported names are `md5`, `sha`, `sha1`, `sha224`, `sha256`,
`sha384` and `sha512`.

Any other name fails closed. A null value
reads as null. A wrong argument count reads as null.
