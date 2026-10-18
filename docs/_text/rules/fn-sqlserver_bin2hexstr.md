`sqlserver_bin2hexstr(<b>)` converts the string `b` to `0x` hex text.

The call takes one argument. It is `bin2hexstr(<b>, '0x')` under
another name. Each byte becomes two upper-case hex digits. An empty
string reads as `0x`. A null value reads as null. A wrong argument
count reads as null.
