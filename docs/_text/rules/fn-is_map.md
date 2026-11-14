`is_map(<v>)` reports whether the value `v` is an object.

The call takes one argument. A JSON object returns `true`. JSON text
that holds an object returns `false`; convert it with `map(<v>)` first.
An array returns `false`. A wrong argument count reads as null.
