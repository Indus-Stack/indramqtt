`null()` returns the null value.

The name `null` is a SQL keyword. The front end reads it as the null
literal, so the call does not parse. The broker registers the name, but
a rule cannot reach it through SQL.

BUG(parity): `null()` is registered, but the SQL front end reads `null`
as a keyword, so a rule cannot call it.
