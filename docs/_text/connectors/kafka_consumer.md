`kafka_consumer` names the Kafka ingress source. The v5 source store keeps its registration and enable flag. The v5 connectors route also maps the name to the `kafka` sink runtime. The v1 route refuses the name.

This build has no Kafka pull loop. The source reads no topic. The tier of the name is community.
