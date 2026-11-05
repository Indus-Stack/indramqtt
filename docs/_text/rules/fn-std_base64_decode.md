`std_base64_decode(s)` is the internal alias of `base64_decode(s)`.

The engine rewrites `base64_decode` to `std_base64_decode` before parsing
when the name collides with a built-in. Use `base64_decode` in rules.
Invalid base64 or non-UTF-8 bytes read as null. A null value reads as
null. A wrong argument count reads as null.
