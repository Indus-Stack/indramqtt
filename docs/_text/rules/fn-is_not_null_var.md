`is_not_null_var(<v>)` reports whether the value `v` is not null.

The call takes one argument. A null payload field returns `false`. An
absent payload field reads as null and returns `false`. A present value
returns `true`. The result matches `is_not_null(<v>)`. A wrong argument
count reads as null.
