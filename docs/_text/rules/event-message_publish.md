`message_publish` alone is not an event source.

Only a full `$events/...` name selects events. As a bare `FROM` name it is a stream. A stream constrains nothing. The rule matches every message.

Match MQTT publishes with a quoted topic source instead. See `clause FROM`.
