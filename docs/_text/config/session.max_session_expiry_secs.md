`session.max_session_expiry_secs` caps the session expiry interval that a client asks for in CONNECT. The broker stores the smaller of the client value and the cap.

0 removes the cap. The default 86400 seconds is one day. The cap applies at CONNECT only.
