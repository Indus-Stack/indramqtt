`zip_compress(<v>)` returns the zlib form of `v` as base64 text.

The call takes one argument. It is an alias of `zip(<v>)`. It reads
the value as bytes. It compresses the bytes with zlib. A null value
reads as null. An input above one mebibyte reads as null. A wrong
argument count reads as null.
