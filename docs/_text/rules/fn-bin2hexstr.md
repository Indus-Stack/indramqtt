`bin2hexstr(<b>)` converts the string `b` to upper-case hex text.
`bin2hexstr(<b>, <prefix>)` prepends `prefix` to the result.

The call takes one or two arguments. Each byte of the string becomes
two upper-case hex digits. An empty string reads as `""`. A null value
reads as null. A wrong argument count reads as null.
