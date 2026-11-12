`webhook` posts rule results to an HTTP endpoint. The v1 route accepts `webhook`, `http`, and `rest`, and stores the runtime as `webhook`. The v5 route accepts `type` `webhook` and registers a minimal sender. The tier is community.

The v1 sink supports POST, PUT, and PATCH. It supports Basic, Bearer, and API-key auth, three body formats, and HMAC request signatures. It retries throttles and server faults with backoff. It holds a bounded queue. The v5 route reads only the URL and sends a raw POST with topic and QoS headers.
