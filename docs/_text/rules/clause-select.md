`SELECT` names the output columns of a rule.

Each expression reads payload fields, calls functions, or computes
values. `AS` renames the column. `*` keeps the full payload object.

A missing field projects as null. The rule still matches. A payload
that is not a JSON object never matches a rule with `SELECT`.
