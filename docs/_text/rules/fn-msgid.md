`msgid()` reads the id of the message under evaluation.

The broker supplies the id from the message. A message with no id reads
as null.

The SQL studio has no live message. The sample payload field `id` or
`msgid` then supplies the value, and the value keeps the type of that
field.

TODO(parity): does a live message supply the id when the payload carries
no `id` field?
