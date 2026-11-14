`$events/session_subscribed` is one row per subscribe.

Name it in `FROM` to run the rule on each subscribe. The row carries `clientid`, `topic`, `qos`, `nl`, `rap` and `rh`.

An MQTT message never matches this source. The engine drops it before evaluation.
