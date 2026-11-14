`pgsql` is the v5 name for the PostgreSQL sink. The v5 route builds a connection URL from `server`, `database`, `username`, and `password`. The v1 route refuses the name. The tier is community.

The v5 route fixes the SQL template, the pool size, the batch size, and the batch timeout. It inserts into `test_telemetry`. The `postgres` page documents the full sink settings.
