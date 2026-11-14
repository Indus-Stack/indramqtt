`POST /api/v1/auth/users` stores one username and password in a tenant.

An empty username or password fails with 400. A create for an existing username replaces the stored credential. The password is write-only and never appears in a read. A password longer than 8192 bytes is refused. A successful write returns 201 with the username and quota object. The write persists and clears cached authorization decisions.
