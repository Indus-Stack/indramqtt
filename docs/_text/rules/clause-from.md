`FROM` names the inputs of a rule. Each message on a named input is one row.

A source is a quoted MQTT topic filter (`"sensors/+"`), an event
source (`"$events/client_connected"`), or a bare stream name.
Several filters mean a message on any filter is one row.

A message outside every source never matches. An event source never
matches an MQTT message. A bare stream name constrains nothing.
