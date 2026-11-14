`contains_topic(<filters>, <topic>)` tests a subscription list for a topic.

The list holds exact topic strings, or objects with `topic` and `qos`.
The match is exact: a wildcard in the list does not match. With a third
argument the stored `qos` must equal the wanted `qos`.
