`mongo_date([<epoch>[, <unit>]])` renders an epoch value as date text.

The call takes no argument, an epoch, or an epoch with a unit. Without
an argument the call uses the current time. A single epoch counts
milliseconds. The accepted units are `second`, `millisecond`,
`microsecond` and `nanosecond`, with the usual short forms. A
non-numeric epoch, a wrong unit or a wrong argument count reads as
null.
