`OR` keeps a row when at least one side is true.

Each comparison runs against the same row. The row matches when
either side is true.

Two false sides mean no match. `AND` binds tighter than `OR`, so
`a OR b AND c` reads as `a OR (b AND c)`. Parentheses override this.
