`session.topic_alias_maximum` sets the largest inbound topic alias that the broker advertises in CONNACK. The value is the v5 Topic Alias Maximum property.

0 disables inbound aliases. A publish with an alias above the value is refused. The default is 10.
