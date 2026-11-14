`now_timestamp([<unit>])` returns the current epoch value as an integer.

The call takes no argument or one unit. The unit sets the scale of
the result. The default unit is `second`. The accepted units are
`second`, `millisecond`, `microsecond` and `nanosecond`, with the
usual short forms. A wrong unit or a wrong argument count reads as
null.
