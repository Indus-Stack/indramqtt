`std_length(<value>)` returns the size of a list or a text.

The call is the internal name of `length`. The broker rewrites SQL
text that names `length` to `std_length` before it parses the query.
A list returns its element count. A text returns its character count.
Any other value returns null.
