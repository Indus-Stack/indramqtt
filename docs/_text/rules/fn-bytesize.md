`bytesize(<b>)` returns the byte length of the string `b`.

The argument must be a string. The result is the UTF-8 byte length of
the string. An empty string reads as 0. A number, an array, or any
other non-string argument reads as null. A wrong argument count reads
as null. A multi-byte character counts as more than one byte.
