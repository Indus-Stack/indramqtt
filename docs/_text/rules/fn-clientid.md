`clientid()` reads the client id that published the message.

A message with no client id reads as null. The SQL studio has no live
client. The sample payload field `clientid` then supplies the value.

TODO(parity): does a live message supply the client id when the payload
carries no `clientid` field?
