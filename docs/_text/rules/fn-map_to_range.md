`map_to_range(<value>, <min>, <max>)` maps a value into [`min`, `max`].

The call takes three arguments. The bounds must be integers. A
number maps by its value modulo the span. A text maps by its first
16 bytes as an unsigned big-endian number modulo the span.

The span
is `max` minus `min` plus one. An empty text reads as null. A `min`
above `max` reads as null. A value that is not a number or text
reads as null. A wrong argument count reads as null.
