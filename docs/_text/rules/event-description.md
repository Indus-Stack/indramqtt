`description` alone is not an event source.

It is a field name of the event catalogue entry. As a bare `FROM` name it is a stream. A stream constrains nothing. The rule matches every message.

Select events only with a full `$events/...` name.
