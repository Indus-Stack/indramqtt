`GROUP BY` groups rows that share column values. Aggregates then fold each group into one row.

A window call (`TUMBLINGWINDOW`, `HOPPINGWINDOW`, `SLIDINGWINDOW`,
`COUNTWINDOW`) buckets rows in time or in count. Plain expressions
group by value.

One message forms one group. Its aggregates see one row, so
`count(field)` returns 1. A batch of messages returns one row per
group. See `HAVING` for filtering groups.
