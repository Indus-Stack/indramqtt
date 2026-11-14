`LIMIT` requests at most N result rows.

The broker parses the clause and accepts the rule. A single-message
test returns its row unchanged, also with `LIMIT 0`. No row trimming
was observed through the test entry. Treat `LIMIT` as accepted but
not applied until proven otherwise.

TODO(parity): does a live windowed rule trim its output rows to `LIMIT`?
TODO(parity): does a live `LIMIT` rule keep its `FROM` topic filter?
BUG(parity): `LIMIT` parses, but no code path reads the parsed limit.
