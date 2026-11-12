`hash_to_range(<key>, <min>, <max>)` maps a text key into [`min`, `max`].

The call takes three arguments. The key must be text. The bounds
must be integers. The broker hashes the key with SHA-256. It returns
`min` plus the remainder modulo the span.

The span is `max` minus
`min` plus one. An empty key reads as null. A non-text key reads as
null. A `min` above `max` reads as null. A wrong argument count
reads as null.
