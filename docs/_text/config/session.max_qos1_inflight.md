`session.max_qos1_inflight` bounds the unacknowledged QoS 1 downlinks in the fast window of each session. Overflow moves to the spill buffer.

The live session reports the bound as `inflight_max`. The default is 100.
