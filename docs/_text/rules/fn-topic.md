`topic()` reads the topic of the message under evaluation.

`topic(<level>)` reads one topic level. Levels count from 1. A level
outside the topic reads as null. A level below 1 reads as null.

TODO(parity): does a live rule read the message topic, and not a payload
`topic` field?
