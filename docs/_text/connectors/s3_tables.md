`s3_tables` writes each result partition group to an Amazon S3 Tables table as an Iceberg data file. The v1 route accepts `s3_tables`, `s3tables`, and `iceberg`. The v5 route accepts `s3_tables` and `s3tables`. The tier is enterprise.

The v1 route reads the full sink settings. The v5 route reads the table bucket ARN, the namespace, the table, the region, the endpoint, the credentials, and the timeout. It fixes the codec, the partition spec, and the batch policy. The `s3_tables` page documents both shapes.
