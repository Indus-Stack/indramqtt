`$events/message_dropped` is one row per dropped message.

Name it in `FROM` to run the rule on each drop. The row carries `clientid`, `topic`, `qos` and `reason`. The kernel fires it at quota shed and at detached clean sessions.

An MQTT message never matches this source. The engine drops it before evaluation.
