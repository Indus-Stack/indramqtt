`sha(<s>)` returns the SHA-1 hex digest of `s`.

The call takes one argument. It hashes the text with SHA-1. It
returns lower-case hex text. An empty text returns the empty digest.
A null value reads as null. A wrong argument count reads as null.
