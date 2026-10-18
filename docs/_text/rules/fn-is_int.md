`is_int(<v>)` reports whether the value `v` is an integer.

The call takes one argument. A JSON integer returns `true`. A float
such as `1.5` or `3.0` returns `false`. A numeric text returns
`false`. Every other value returns `false`. A wrong argument count
reads as null.
