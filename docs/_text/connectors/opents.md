`opents` is the v5 name for the `opentsdb` sink. The v5 route maps the name to the `opentsdb` runtime and keeps the name on the stored row. The v1 route refuses the name. The tier is community.

The v5 route reads the endpoint, the metric, the value field, the summary flag, the tag map, and the timeout. It fixes the protocol, the compression, and the batch size. The `opentsdb` page documents the full sink settings.
