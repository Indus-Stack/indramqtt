`getenv(<name>)` returns the value of a process environment variable.

The call takes one name. A missing variable reads as an empty text. A
name longer than 4096 characters reads as null. A non-text name or a
wrong argument count reads as null.
