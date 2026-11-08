`snowflake` writes each result row to a Snowflake table through the Snowpipe Streaming rows API. The v1 route accepts `snowflake`, `snow`, and `sf`. The v5 route accepts `snowflake`. Both routes store the runtime as `snowflake`. The tier is enterprise.

The sink mints an RS256 JWT per flush from the private key. It posts one JSON row array per table group to the channel rows endpoint. The v1 route reads the full sink settings. The v5 route reads the account, user, database, schema, table, key, endpoint, and timeout. It fixes the channel, the role, the column map, and the batch policy.
