`gunzip(<s>)` returns the gzip source of `s` as text.

The call takes one argument. It decodes base64 first. It uses raw
bytes when base64 fails. It decompresses the bytes with gzip. It
returns text. A null value reads as null. Bad bytes read as null.
Non-text bytes read as null. An output above one mebibyte reads as
null. A wrong argument count reads as null.
