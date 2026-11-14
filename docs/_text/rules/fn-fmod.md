`fmod(<x>, <y>)` returns the floating remainder of x divided by y.

The result takes the sign of the dividend. The broker converts each
argument to a number first: `true` is 1, `false` is 0, and text must hold
a number. A zero divisor, a value of another type, and text that is not a
number read as null.
