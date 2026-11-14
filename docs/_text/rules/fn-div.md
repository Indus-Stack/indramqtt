`div(<x>, <y>)` divides two integers and returns the integer quotient.

The result truncates toward zero. The broker converts each argument to an
integer first: a decimal loses its fraction, `true` is 1, and `false` is
0. Text must hold an integer. A zero divisor, a value of another type,
and text that is not an integer read as null.
