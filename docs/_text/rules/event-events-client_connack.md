`$events/client_connack` is one row per answered `CONNECT`.

Name it in `FROM` to run the rule on each answer. The row carries `clientid`, `return_code` and `session_present`.

An MQTT message never matches this source. The engine drops it before evaluation.
