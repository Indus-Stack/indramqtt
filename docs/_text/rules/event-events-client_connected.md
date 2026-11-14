`$events/client_connected` is one row per accepted connection.

Name it in `FROM` to run the rule on each connect. The row carries `clientid`, `keepalive`, `proto_ver` and `clean_start`.

An MQTT message never matches this source. The engine drops it before evaluation.
