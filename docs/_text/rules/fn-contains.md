`contains(<element>, <list>)` reports whether a list holds an element.

The call takes two arguments. It returns `true` when the list holds a
value equal to the first argument. It returns `false` when the list
does not hold that value. A value that is not a list returns null.
Equality is strict: the integer `1` and the float `1.0` are different
values.

TODO(parity): the open question is whether the reference compares an
integer and a float as equal. The broker compares them strictly and
reports `false`.
