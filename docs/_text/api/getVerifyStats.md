`GET /api/v1/auth/verify_stats` reads the password verification counters of the process.

`verifications` counts blocking password hashes. `dedup_joins` counts CONNECTs that shared one leader check. The counters live in memory and restart at zero.
