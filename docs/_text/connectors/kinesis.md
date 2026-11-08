`kinesis` sends each result row to an Amazon Kinesis stream with `PutRecords`. Both the v1 and the v5 connectors routes accept the name. The tier is enterprise.

The v1 route reads the full sink settings. The v5 route fixes the session token, the partition key template, the batch limits, and the retry values. The `kinesis` page documents both shapes.
