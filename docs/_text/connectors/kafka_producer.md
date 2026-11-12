`kafka_producer` is the documented producer name for the `kafka` sink. The v5 connectors route maps the name to the `kafka` runtime and keeps the name on the stored row. The v1 route refuses the name. The tier of the name is community.

The v5 route fixes the topic template, the partition key, the partition count, the client id, the acknowledgement mode, and the batch limits. The `kafka` page documents the full sink settings.
