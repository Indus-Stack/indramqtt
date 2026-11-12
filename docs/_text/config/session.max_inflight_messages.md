`session.max_inflight_messages` is the documented maximum of inflight unacknowledged QoS 1 and QoS 2 messages for a session.

The boot path does not read this field. The live window comes from `session.max_qos1_inflight`.
