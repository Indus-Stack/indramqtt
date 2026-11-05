`lz4_uncompress(<s>)` returns the LZ4 source of `s` as text.

The call takes one argument. It decodes base64 first. It uses raw
bytes when base64 fails. It decompresses the frame with LZ4. It
returns text. An empty input reads as null. A null value reads as
null. Bad bytes read as null. Non-text bytes read as null. An output
above one mebibyte reads as null. A wrong argument count reads as
null.
