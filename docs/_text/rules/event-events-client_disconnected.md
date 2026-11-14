`$events/client_disconnected` is one row per disconnect.

Name it in `FROM` to run the rule on each close. The row carries `clientid`, `reason` and `disconnect_code`. The code is absent when the edge sends none.

An MQTT message never matches this source. The engine drops it before evaluation.
