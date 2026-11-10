`listeners.tcp.max_connections` caps concurrent connections on the plaintext listener. The kernel closes a new connection past the cap. The same cap also sizes the TLS, WS and WSS rows.

The default is 1000000.
