`redshift` writes each result row to an Amazon Redshift database with the Redshift Data API. Both connector routes accept the name. The tier is enterprise.

The v1 route reads the full sink settings. The v5 route reads the database, the table, the workgroup, the region, the endpoint, the credentials, and the timeout. It fixes the cluster, the user, the custom SQL, the batch policy, and the retry values. The `redshift` page documents both shapes.
