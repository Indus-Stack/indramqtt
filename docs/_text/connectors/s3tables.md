`s3tables` is an accepted name for the `s3_tables` sink. The v1 route accepts `s3tables` and `iceberg`, and stores the runtime as `s3_tables`. The v5 route accepts `s3tables`, stores the posted type, and registers the aliases `s3_tables:name` and `s3tables:name`. The runtime tier is enterprise.

The v1 route reads the full sink settings. The v5 route reads the table bucket ARN, the namespace, the table, the region, the endpoint, the credentials, and the timeout. It fixes the codec, the partition spec, and the batch policy. The `s3_tables` page documents the shared sink.
