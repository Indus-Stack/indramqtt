`float(<v>)` converts the value `v` to a number.
`float(<v>, <decimals>)` rounds the result to `decimals` places.

The call takes one or two arguments. A numeric text parses. A boolean
reads as 1.0 or 0.0. `decimals` runs from 0 to 20; a value outside that
range reads as null. Every other value reads as null. A wrong argument
count reads as null.
