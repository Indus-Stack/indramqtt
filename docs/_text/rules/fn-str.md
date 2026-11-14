`str(<v>)` converts the value `v` to a string.

The call takes one argument. A number or a boolean renders as plain
text. An array or an object renders as JSON text. A null value reads
as null. A wrong argument count reads as null.

TODO(parity): the rendering of a nested array or object is open. The
broker renders it as JSON text.
