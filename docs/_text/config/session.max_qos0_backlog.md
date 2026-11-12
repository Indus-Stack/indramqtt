`session.max_qos0_backlog` bounds the queued QoS 0 frames of each subscriber. Past the bound, the oldest queued frame drops and the shed counter rises.

The bound applies to live egress only. QoS 1 and QoS 2 frames never shed. The default is 1000 frames.
