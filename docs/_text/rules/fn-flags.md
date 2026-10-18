`flags()` reads the flag map of the message under evaluation.

The value is an object. A message with no flags reads as an empty
object `{}`.

TODO(parity): does a live message supply its flag map when the payload
carries no `flags` field?
