# IndraMQTT on Docker Hub

`indusstack/indramqtt:2026.11.0-beta.1` is the pinned Beta 2026.11 image.
The image contains the IndraMQTT broker and `indra` operator CLI, with the full
connector feature set. The beta is for single-node evaluation.

```sh
git clone https://github.com/Indus-Stack/indramqtt.git
cd indramqtt
git checkout v2026.11.0-beta.1
docker pull indusstack/indramqtt:2026.11.0-beta.1
docker compose up -d --no-build --wait
curl -fsS http://localhost:18083/healthz
```

The Compose example exposes MQTT on port 1883, WebSocket on 8083, and the
management API on 18083. It permits anonymous MQTT clients for evaluation;
protect the ports and change the default administrator password before wider
use. The [release notes](https://github.com/Indus-Stack/indramqtt/blob/v2026.11.0-beta.1/RELEASE_NOTES_2026.11.md) list current limits,
and the [Compose guide](https://github.com/Indus-Stack/indramqtt/blob/v2026.11.0-beta.1/deploy/compose/README.md) covers login and an MQTT round
trip. Pin the versioned image tag for deployments.
