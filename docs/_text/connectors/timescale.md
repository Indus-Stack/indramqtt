`timescale` is the documented short name for the TimescaleDB sink. The v1 route accepts `timescale` and `timescaledb`, stores the runtime as `timescaledb`, and answers with `timescaledb`. The v5 route accepts `type` `timescale`, stores the posted type, and registers no live sink. The tier is community.

The sink writes each result as one hypertable row. It sends bound parameters through the maintained PostgreSQL driver. It buffers rows and flushes full or stale batches. The v1 route reads the full sink settings. The v5 route stores the body and applies no setting.
