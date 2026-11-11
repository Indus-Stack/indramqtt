`DELETE /api/v1/auth/users/{username}` removes one user from a tenant.

A successful delete returns 204 with no body. An unknown username returns 404. The delete persists.
