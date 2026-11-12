`$events/message_publish` is not an event of this broker.

The event catalogue omits this name. No kernel producer fires it. A rule that names it never runs in production.

Match MQTT publishes with a quoted topic source instead. See `clause FROM`.
