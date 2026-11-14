`WHERE` keeps only the rows that pass a boolean test.

The predicate compares fields (`temperature > 80.0`), combines tests
(`AND`, `OR`, `NOT`), and matches text (`LIKE`, `IN`, `BETWEEN`,
`IS NULL`).

A false predicate means no match. The rule then produces no row.
A missing field reads as null, so a comparison against it does
not match.
