`peerhost()` reads the host of the client that published the message.

The broker supplies the host from the message. A message with no host
reads as null.

The SQL studio has no live client. The sample payload field `peerhost`
then supplies the value, and the value keeps the type of that field.

TODO(parity): does a live message supply the peer host when the payload
carries no `peerhost` field?
