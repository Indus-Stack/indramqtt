`flag('<name>')` reads one flag of the message under evaluation.

The first argument is the flag name. An absent flag name reads as null.

TODO(parity): does a live message supply its flags when the payload
carries no `flags` field?
