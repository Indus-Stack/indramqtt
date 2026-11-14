`str_utf8(<v>)` converts the value `v` to UTF-8 text.

The call is the same as `str(<v>)`. A JSON string is UTF-8 text by
construction. A number or a boolean renders as plain text. A null
value reads as null. A wrong argument count reads as null.
