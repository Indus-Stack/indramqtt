`payload()` reads the payload of the message under evaluation.

`payload('<json path>')` reads one value at a dotted path. An absent
path reads as null. A path on a payload that is not an object reads as
null.
