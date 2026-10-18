`username()` reads the username that published the message.

The broker supplies the username from the message. A message with no
username reads as null.

The SQL studio has no live client. The sample payload field `username`
then supplies the value, and the value keeps the type of that field.

TODO(parity): does a live message supply the username when the payload
carries no `username` field?
