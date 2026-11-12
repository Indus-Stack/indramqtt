`prometheus.latency_buckets` sets the latency bucket list text. The broker stores the text and shows it in `GET /api/v5/prometheus`. No histogram reads it.

The default is `10ms, 100ms, 1s, 5s, 30s`.
