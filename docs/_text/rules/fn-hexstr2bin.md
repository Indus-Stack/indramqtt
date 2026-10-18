`hexstr2bin(<s>)` converts the hex text `s` to a string.
`hexstr2bin(<s>, <prefix>)` strips the required `prefix` first.

The call takes one or two arguments. The text must hold hex digit
pairs. A missing prefix, an odd length, or a non-hex digit reads as
null. The decoded bytes must form valid UTF-8 text; any other result
reads as null. A wrong argument count reads as null.
