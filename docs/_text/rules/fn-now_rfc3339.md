`now_rfc3339([<unit>])` returns the current time as RFC 3339 text.

The call takes no argument or one unit. The unit sets the fraction
precision. The default unit is `second`. The accepted units are
`second`, `millisecond`, `microsecond` and `nanosecond`, with the usual
short forms. A wrong unit or a wrong argument count reads as null.
