`qos()` reads the QoS level of the message under evaluation.

The value is the number 0, 1 or 2. A message with no QoS reads as null.

The SQL studio has no live message. The sample payload field `qos` then
supplies the value.

TODO(parity): does a live message supply the QoS when the payload carries
no `qos` field?
