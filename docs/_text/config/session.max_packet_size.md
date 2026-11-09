`session.max_packet_size` sets the largest MQTT packet that the broker accepts. It also sets the WebSocket frame cap.

The value is advertised in the v5 CONNACK Maximum Packet Size property. An oversize packet closes the connection. The default is 1048576 bytes.
