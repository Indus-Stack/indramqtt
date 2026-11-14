`dynamodb` writes each result row as a DynamoDB item.

The sink puts items into `table_name`. `partition_key` and `sort_key` map the keys. `ttl_attribute` with `ttl_secs` expires items. The tier is enterprise.
