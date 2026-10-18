`bitsr(<x>, <n>)` shifts the bits of the integer `x` right by `n` places.

Each argument must convert to an integer. A number with a fraction
truncates toward zero, so `bitsr(4.9, 1)` is 2. The shift count `n` runs
from 0 to 63, and a count outside that range reads as null. A wrong
argument count reads as null. The shift keeps the sign: a negative value
stays negative.
