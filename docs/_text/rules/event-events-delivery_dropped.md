`$events/delivery_dropped` is one row per missed delivery.

Name it in `FROM` to run the rule on each miss. The row carries `clientid`, `topic`, `qos` and `reason`. The kernel fires it at route failure for dead connections.

An MQTT message never matches this source. The engine drops it before evaluation.
