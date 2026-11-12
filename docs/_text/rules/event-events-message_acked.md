`$events/message_acked` is one row per client acknowledgement.

Name it in `FROM` to run the rule on each ack. The row carries `clientid`, `topic`, `qos` and `msg_id`. The kernel fires it at `PUBACK`.

An MQTT message never matches this source. The engine drops it before evaluation.
