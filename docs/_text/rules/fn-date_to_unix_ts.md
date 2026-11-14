`date_to_unix_ts(<unit>, [<offset>, ]<pattern>, <input>)` parses date text to an epoch value.

The 3-arg form takes the unit, the pattern, and the input text. The
4-arg form takes the unit, the offset, the pattern, and the input text. The offset sets the zone: a fixed offset such as `Z` or
`+08:00`, or seconds as a number. The parser reads the numbers of the
input in pattern order. A wrong unit, an input with too few numbers,
or a wrong argument count reads as null. A named zone fails closed.
