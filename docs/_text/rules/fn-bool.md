`bool(<v>)` converts the value `v` to a boolean.

The call takes one argument. `true` and `false` stay. The numbers `1`
and `0` convert. The texts `true`, `false`, `1` and `0` convert. Letter
case and surrounding spaces do not change the result. Every other value
reads as null. A wrong argument count reads as null.

TODO(parity): the full truthiness table is open. The broker accepts
only the values above.
