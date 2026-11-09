`session.max_qos1_spill` bounds the QoS 1 messages that wait past the inflight window. The broker tracks them for a DUP replay.

A session tracks at most the window plus the spill. A message past both bounds stays untracked. The default is 1000.
