`mqtt` is the documented name for the `mqtt_bridge` sink. The v5 connectors route maps the name to the `mqtt_bridge` runtime and keeps the name on the stored row. The v1 route refuses the name. The tier is community.

The v5 route reads the remote address, the client id, the username, and the password. It fixes the remaining bridge settings. The `mqtt-bridge` page documents the full sink settings.
