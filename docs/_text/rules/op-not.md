`NOT` negates a boolean test.

The row matches when the inner test is false. `NOT` also prefixes
range tests: `NOT BETWEEN`, `NOT IN`, and `NOT LIKE` bind at the
range level.

Parentheses set the scope: `NOT (a AND b)` negates the whole group.
A missing field reads as null, and `NOT` on it still matches.
