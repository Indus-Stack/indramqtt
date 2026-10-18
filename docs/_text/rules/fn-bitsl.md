`bitsl(<x>, <n>)` shifts the bits of the integer `x` left by `n` places.

Each argument must convert to an integer. A number with a fraction
truncates toward zero, so `bitsl(1.9, 2)` is 4. The shift count `n` runs
from 0 to 63, and a count outside that range reads as null. A wrong
argument count reads as null. The result is `x * 2^n` modulo 2^64, read
as a signed 64-bit integer. An overflowing shift wraps and never reads
as null: `bitsl(3, 63)` is -9223372036854775808.
