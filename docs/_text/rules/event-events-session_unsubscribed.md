`$events/session_unsubscribed` is one row per unsubscribe.

Name it in `FROM` to run the rule on each unsubscribe. The row carries `clientid` and `topic`.

An MQTT message never matches this source. The engine drops it before evaluation.
