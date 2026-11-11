`GET /api/v1/auth/acls` lists the ordered ACL rules of a tenant.

The rules keep evaluation order. The first match wins. An absent `tenant` reads the default tenant. A tenant with no rules returns an empty list.
