`str_utf16_le(<v>)` converts the value `v` to base64 UTF-16LE text.

The call takes one argument. The broker encodes the string as UTF-16LE
bytes and returns those bytes as base64 text. An empty string reads as
`""`. A null value reads as null. A wrong argument count reads as null.

TODO(parity): the wire form of a UTF-16LE value is open. The broker
returns base64 text.
