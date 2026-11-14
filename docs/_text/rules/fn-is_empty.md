`is_empty(<v>)` reports whether the value `v` is empty.

The call takes one argument. An empty text, an empty array and an
empty object each return `true`. The null value and an absent payload
field return `true`. A number and a boolean never return `true`. A
wrong argument count reads as null.

TODO(parity): the open question is whether the number 0 and the
boolean false count as empty. The broker reads them as not empty.
