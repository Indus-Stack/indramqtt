`sublist(<len>, <list>)` returns the first `len` elements.
`sublist(<start>, <len>, <list>)` returns a slice from `start`.

The call takes two or three arguments. The two-argument form takes the
first `len` elements. The three-argument form counts from 1 and clamps
at the end of the list. A negative `len`, a negative two-argument
count, or a `start` below 1 returns null.
