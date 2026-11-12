`gzip(<v>)` returns the gzip form of `v` as base64 text.

The call takes one argument. It reads the value as bytes. It
compresses the bytes with gzip. It encodes the result with base64.
A null value reads as null. An input above one mebibyte reads as
null. A wrong argument count reads as null.
