`HAVING` keeps only the groups that pass a boolean test. It runs after `GROUP BY`.

The predicate may call aggregates (`HAVING count(temperature) > 1`).
A group that fails the test produces no row.

A batch of two rows in one group passes `count(temperature) > 1`.
A batch of one row does not pass it. A single message returns its
row without group filtering.

TODO(parity): does `HAVING` filter a single non-windowed row on the
live path, or only grouped batches and windows?
