`LIKE` keeps a row when text matches a pattern.

`%` matches any run of characters, including none. `_` matches exactly
one character. `NOT LIKE` negates the test.

Matching is case-sensitive: `'KIT%'` does not match `kitchen-1`.
A non-text value never matches a pattern.
