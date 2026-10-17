`IN` keeps a row when a value equals any member of a list.

Members are literals such as numbers or quoted text. A value outside
the list does not match. `NOT IN` negates the test.

An empty list matches nothing. A missing value reads as null, so it
matches no member and `NOT IN` keeps the row.
