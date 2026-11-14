`clientip()` reads the IP address of the client that published the message.

`clientip()` is an alias of `peerhost()`. The SQL studio has no live
client. The sample payload field `peerhost` then supplies the value.

TODO(parity): does a live message supply the peer address, as `peerhost()`
does?
