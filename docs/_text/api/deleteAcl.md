`DELETE /api/v1/auth/acls/{id}` removes one ACL rule at an index in a tenant.

`id` is the zero-based index in the tenant rule list. A successful delete returns 204 with no body. An index past the end returns 404. The delete persists and clears cached authorization decisions.
