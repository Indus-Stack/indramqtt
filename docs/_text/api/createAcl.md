`POST /api/v1/auth/acls` appends one ordered ACL rule to a tenant.

A tenant holds at most 10000 rules. An empty client pattern fails with 400. The action must be `publish`, `subscribe` or `all`. The topic pattern must be a valid MQTT filter. A successful write returns 201 with the stored client pattern, topic pattern and allow value. The write persists and clears cached authorization decisions.
