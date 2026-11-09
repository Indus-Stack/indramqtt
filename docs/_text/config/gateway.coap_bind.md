`gateway.coap_bind` sets the UDP address of the CoAP gateway. An empty value disables the gateway.

A CoAP POST to `/ps/<topic>` publishes through the router. Subscribers receive the payload on the inner topic. The gateway and the subscribers share the default tenant.
