`length(<value>)` returns the size of a list or a text.

The call takes one argument. A list returns its element count. A text
returns its character count. Any other value returns null. The engine
rewrites the name `length` to the internal alias `std_length` before
it parses the SQL text.
