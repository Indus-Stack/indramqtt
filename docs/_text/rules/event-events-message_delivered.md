`$events/message_delivered` is one row per delivery to a subscriber mailbox.

Name it in `FROM` to run the rule on each delivery. The row carries `clientid`, `topic`, `qos`, `retain`, `payload` and `msg_id`. The kernel fires it at fan-out.

An MQTT message never matches this source. The engine drops it before evaluation.
