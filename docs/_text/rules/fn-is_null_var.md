`is_null_var(<v>)` reports whether the value `v` is null.

The call takes one argument. A null payload field reads as null, so
the call returns `true` for it. A present value returns `false`. The
result matches `is_null(<v>)`. A wrong argument count reads as null.

TODO(parity): the open question is whether `is_null_var` must differ
from `is_null`. The broker returns the same result for both.
