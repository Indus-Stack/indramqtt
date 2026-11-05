`base64_decode(s)` base64-decodes text `s` to UTF-8 text.

The call takes one to three arguments. The first is the base64 text to
decode. Extra arguments are options: `urlsafe` and `no_padding`. Invalid
base64 or non-UTF-8 bytes read as null. A null value reads as null. A
wrong argument count reads as null.
