`listeners.tcp.backlog` sizes the accept queue of the plaintext listener. The kernel passes the value to the socket at bind. No API reports the live value.

The default is 1024.
