`std_base64_encode(v)` is the internal alias of `base64_encode(v)`.

The engine rewrites `base64_encode` to `std_base64_encode` before parsing
when the name collides with a built-in. Use `base64_encode` in rules. A
null value reads as null. A wrong argument count reads as null.
