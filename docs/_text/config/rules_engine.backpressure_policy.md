`rules_engine.backpressure_policy` sets the default memory backpressure policy when the rule buffer is full. The values are Block, DropNewest, DropOldest, SpillToDisk and RejectPublisher.

The default is DropOldest.
Read the live value from `GET /api/v1/nodes`.
