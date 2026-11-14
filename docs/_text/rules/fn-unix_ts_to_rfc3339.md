`unix_ts_to_rfc3339(<epoch>[, <unit>])` renders an epoch value as RFC 3339 text.

The call takes the epoch and an optional unit. The default unit is
`second`. The accepted units are `second`, `millisecond`,
`microsecond` and `nanosecond`, with the usual short forms. A
non-numeric epoch, a wrong unit or a wrong argument count reads as
null.
