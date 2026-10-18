`is_not_null(<v>)` reports whether the value `v` is not null.

The call takes one argument. It returns `true` for every value that is
present, also for an empty text. The null value returns `false`. A
wrong argument count reads as null.
