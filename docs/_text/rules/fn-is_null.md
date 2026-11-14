`is_null(<v>)` reports whether the value `v` is null.

The call takes one argument. It returns `true` only for the null value.
An absent payload field reads as null, so the call returns `true` for
it. A present value returns `false`. A wrong argument count reads as
null.

TODO(parity): the open question is whether `is_null` and
`is_null_var` must return different results. The broker reads both the
same way.
