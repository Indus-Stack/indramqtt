`prometheus.push_gateway.job_name` sets the push job name template. The broker stores the template and shows it in `GET /api/v5/prometheus`. It runs no push client.

The default is `${name}/instance/${name}~${host}`.
