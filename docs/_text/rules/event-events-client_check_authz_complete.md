`$events/client_check_authz_complete` is one row per completed authorization check.

Name it in `FROM` to run the rule on each verdict. The row carries `clientid`, `action`, `topic`, `qos`, `allow` and `source`. It carries `retain` only for a publish check.

An MQTT message never matches this source. The engine drops it before evaluation.
