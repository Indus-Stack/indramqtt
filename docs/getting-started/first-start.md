---
title: "First start"
description: "Write the configuration and start the broker."
sidebar:
  order: 12
---

# First start

Create a directory for the configuration. Write `indra.toml` in it. The file below starts the plaintext MQTT listener on port 1883 and the management API on port 18083.

```mermaid
flowchart LR
    File["indra.toml"] -- "check" --> Check["--check-config prints ok"]
    Check -- "start" --> Boot["kernel binds :1883 + :18083"]
    Boot -- "ask" --> Health["GET /healthz returns OK"]
```

## Before you start

- Install the binaries from [Install](./install.md).
- Keep ports 1883 and 18083 free.
- Pick an API key with 16 characters or more.

```toml test
[node]
id = "docs-start"

[listeners.tcp]
bind = "0.0.0.0:1883"

[listeners.api]
bind = "0.0.0.0:18083"

[logging]
level = "warn"
```

## Check that it works

Validate the directory before you start. The command prints `ok` and exits 0 for a valid directory. It names the error and exits 2 for an invalid directory.

```sh test
"${INDRAMQTT_BIN:-indramqtt}" --config-dir "${DOCS_CFG_DIR:-./docs-start}" --check-config
```

```output
ok
```

Export two credentials, then start the broker. Each key needs 16 characters or more.

```sh no-test
# Runs until stopped. The example test starts the broker itself, so it does not run this block.
export INDRA_API_KEYS=your-api-key-here
export INDRA_API_KEY=your-api-key-here
indramqtt --config-dir ./docs-start
```

Check that the broker answers. The health endpoint returns `OK`.

```sh test
python3 - <<'PY'
import os, socket
port = int(os.environ.get("API_PORT", "18083"))
sock = socket.create_connection(("127.0.0.1", port), timeout=5)
sock.sendall(b"GET /healthz HTTP/1.0\r\nHost: docs\r\n\r\n")
print(sock.makefile().read().strip().splitlines()[-1])
PY
```

```output
OK
```

## Next steps

Subscribe and publish in [First publish](./first-publish.md). It sends the first message through the broker.
