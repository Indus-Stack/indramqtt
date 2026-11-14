`map(<v>)` converts the value `v` to an object.

The call takes one argument. JSON text that holds an object reads as
that object. A list of `[key, value]` pairs reads as an object with
those keys. A key that is not a text reads as null. Every other value
reads as null. A wrong argument count reads as null.
