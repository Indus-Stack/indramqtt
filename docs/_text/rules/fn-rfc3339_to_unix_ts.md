`rfc3339_to_unix_ts(<text>[, <unit>])` parses RFC 3339 text to an epoch value.

The call takes the text and an optional unit. The default unit is
`second`. The accepted units are `second`, `millisecond`,
`microsecond` and `nanosecond`, with the usual short forms. A text
that is not RFC 3339, a wrong unit or a wrong argument count reads as
null.
