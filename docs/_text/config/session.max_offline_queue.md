`session.max_offline_queue` bounds the offline message queue of each session. Past the bound, the broker drops messages by its oldest-first policy.

The publish path uses the live bound. A detached durable session keeps at most this many queued messages. The default is 50000.
