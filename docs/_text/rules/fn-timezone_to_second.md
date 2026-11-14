`timezone_to_second(<zone>)` returns the offset of a zone in seconds.

The call takes one zone value. Use `Z` or a fixed offset such as
`+08:00`. `timezone_to_second` is an alias of
`timezone_to_offset_seconds`. Both names give the same result. A named
zone fails closed and reads as null. A wrong argument count reads as
null.
