`oci_streaming` writes each result row to an OCI Streaming stream with a signed `PutMessages` call. Both connector routes accept the name. The tier is enterprise.

The v1 route reads the full sink settings. The v5 route reads the identity fields, the endpoint, the timeout, and the partition key template. The `oci_streaming` page documents both shapes.
