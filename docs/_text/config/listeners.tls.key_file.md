`listeners.tls.key_file` points to the PEM private key. The listener needs it when `enabled` is true and PSK is off. The key must match the certificate. Boot fails when the file is missing or empty.
