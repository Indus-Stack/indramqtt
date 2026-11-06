#!/usr/bin/env python3
"""Run doc examples through the real entry. Capture output (DX-01, DX2).

Read definitions from docs/_examples/<area>/<item>.toml.
The input only names a command, a file plus an action,
an API call, or a rule with its input message.
Start the built broker with a fresh data directory when
the example needs one. Run the input through the real
entry. Write docs/_verified/<area>/<item>.json.
On failure write docs/_verification/failures.md. Keep
no verified file for a failed item. Never edit an
example to make it pass. Standard library only.
Run from the workspace root:
  python3 docs/_tools/verify.py --area cli [--write]
  python3 docs/_tools/verify.py --area rules [--write]
  python3 docs/_tools/verify.py --all [--write]
Without --write, check only. It prints items, verified,
failed. It fails when an item has no verified file and
no failure record.
"""
import argparse
import datetime
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "docs" / "_examples"
VERIFIED = ROOT / "docs" / "_verified"
FAILURES = ROOT / "docs" / "_verification" / "failures.md"


def find_binary(name):
    """Locate a built binary. No build here. Return path or None."""
    env = {"indramqtt": "INDRAMQTT_BIN", "indra": "INDRACLI_BIN"}.get(name)
    if env and os.environ.get(env):
        cand = Path(os.environ[env])
        if cand.is_file():
            return str(cand)
    cands = []
    target = os.environ.get("CARGO_TARGET_DIR")
    if target:
        cands.append(Path(target) / "debug" / name)
    cands += [
        Path("/work/target/debug") / name,
        ROOT / "target" / "debug" / name,
        ROOT / "target" / "ci-check" / "debug" / name,
    ]
    for cand in cands:
        if cand.is_file():
            return str(cand)
    return None


def run(argv, timeout=30):
    """Run one command. Return (returncode, stdout+stderr)."""
    try:
        proc = subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout
        )
    except Exception as exc:  # noqa: BLE001
        return -1, f"spawn failed: {exc}"[:300]
    return proc.returncode, (proc.stdout + proc.stderr)[:20000]


def load_example(path):
    """Read one example definition. Fail closed on bad shape."""
    if tomllib is None:
        raise SystemExit("need Python 3.11 or newer for tomllib")
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    ex = data.get("example", {})
    for key in ("area", "item", "kind"):
        if key not in ex:
            raise SystemExit(f"{path}: example misses {key}")
    if ex["kind"] == "rule-test":
        cases = data.get("case", [])
        if not cases:
            raise SystemExit(f"{path}: rule-test needs at least one [[case]]")
        for case in cases:
            for key in ("sql", "topic", "payload", "expect_matched"):
                if key not in case:
                    raise SystemExit(f"{path}: case misses {key}")
            try:
                json.loads(case["payload"])
            except ValueError:
                raise SystemExit(f"{path}: case payload is not JSON")
            if "expect_projected" in case:
                try:
                    json.loads(case["expect_projected"])
                except ValueError:
                    raise SystemExit(f"{path}: expect_projected is not JSON")
        ex["cases"] = cases
    elif ex["kind"] == "action-create":
        cases = data.get("case", [])
        if not cases:
            raise SystemExit(f"{path}: action-create needs at least one [[case]]")
        for case in cases:
            for key in ("name", "type", "expect_status"):
                if key not in case:
                    raise SystemExit(f"{path}: case misses {key}")
            if not isinstance(case["expect_status"], int):
                raise SystemExit(f"{path}: expect_status is not a number")
        ex["cases"] = cases
    else:
        for key in ("binary", "args"):
            if key not in ex:
                raise SystemExit(f"{path}: example misses {key}")
    return ex


def verify_one(ex):
    """Run one example. Return (ok, record dict)."""
    binary = find_binary(ex["binary"])
    if binary is None:
        return False, {
            "item": ex["item"],
            "error": f"binary {ex['binary']} is missing",
        }
    kind = ex["kind"]
    if kind == "help-contains":
        argv = [binary] + list(ex["args"]) + ["--help"]
        code, out = run(argv)
        match = ex.get("match", "")
        lines = [ln for ln in out.splitlines() if match in ln]
        ok = code == 0 and bool(lines)
        return ok, {
            "item": ex["item"],
            "command": argv,
            "returncode": code,
            "match": match,
            "output": "\n".join(lines[:6]),
            "output_bytes": len(out),
        }
    if kind == "help-ok":
        argv = [binary] + list(ex["args"]) + ["--help"]
        code, out = run(argv)
        ok = code == 0
        return ok, {
            "item": ex["item"],
            "command": argv,
            "returncode": code,
            "output": "\n".join(out.splitlines()[:4]),
            "output_bytes": len(out),
        }
    if kind == "check-config":
        with tempfile.TemporaryDirectory(prefix="dx01-") as tmp:
            argv = [binary, "--config-dir", tmp, "--check-config"]
            code, out = run(argv)
        ok = code == 0
        return ok, {
            "item": ex["item"],
            "command": [binary, "--config-dir", "<tmp>", "--check-config"],
            "returncode": code,
            "output": out.strip().splitlines()[-1] if out.strip() else "",
            "output_bytes": len(out),
        }
    return False, {"item": ex["item"], "error": f"unknown kind {kind}"}


def free_port():
    """Take one free loopback port. The caller binds it right away."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class RuleBroker:
    """One built broker for every rule-test example of a run."""

    def __init__(self, binary):
        self.binary = binary
        self.key = "dxrule-" + secrets.token_hex(8)
        self.tmp = tempfile.mkdtemp(prefix="dxrules-")
        self.proc = None
        self.api = None

    def start(self):
        """Boot the broker on free ports. Fail closed when it never answers."""
        mqtt, tls, ws, wss, api = (free_port() for _ in range(5))
        cluster, gossip, quic = (free_port() for _ in range(3))
        cfg = Path(self.tmp) / "cfg"
        (cfg / "conf.d").mkdir(parents=True)
        (cfg / "indra.toml").write_text(
            "[node]\n"
            'id = "dxrules"\n'
            "\n"
            "[listeners.tcp]\n"
            f'bind = "127.0.0.1:{mqtt}"\n'
            "\n"
            "[listeners.api]\n"
            f'bind = "127.0.0.1:{api}"\n'
            "\n"
            "[logging]\n"
            'level = "warn"\n'
            "\n"
            "[auth]\n"
            "allow_anonymous = true\n",
            encoding="utf-8",
        )
        (cfg / "conf.d" / "99-dxrules.toml").write_text(
            "[node]\n"
            f'data_dir = "{Path(self.tmp) / "data"}"\n'
            "[listeners.tcp]\n"
            f'bind = "127.0.0.1:{mqtt}"\n'
            "[listeners.tls]\n"
            f'bind = "127.0.0.1:{tls}"\n'
            "[listeners.ws]\n"
            f'bind = "127.0.0.1:{ws}"\n'
            "[listeners.wss]\n"
            f'bind = "127.0.0.1:{wss}"\n'
            "[listeners.api]\n"
            f'bind = "127.0.0.1:{api}"\n'
            "[cluster]\n"
            f'bind = "127.0.0.1:{cluster}"\n'
            f'gossip_bind = "127.0.0.1:{gossip}"\n'
            f'quic_bind = "127.0.0.1:{quic}"\n',
            encoding="utf-8",
        )
        log = open(Path(self.tmp) / "broker.log", "w", encoding="utf-8")  # noqa: PTH123
        env = dict(os.environ)
        env["INDRA_API_KEYS"] = self.key
        self.proc = subprocess.Popen(  # noqa: S603
            [self.binary, "--config-dir", str(cfg)],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        self.api = api
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                sock = socket.create_connection(("127.0.0.1", api), timeout=2)
                sock.sendall(b"GET /healthz HTTP/1.0\r\nHost: dxrules\r\n\r\n")
                body = sock.makefile().read()
                sock.close()
                if body.strip().splitlines()[-1] == "OK":
                    return
            except OSError:
                time.sleep(0.2)
        raise SystemExit("rule broker never answered /healthz")

    def dry_run(self, sql, topic, payload):
        """Run one rule test call. Return (status, body dict or text)."""
        data = json.dumps(
            {"sql_query": sql, "topic": topic, "payload": payload}
        ).encode()
        req = urllib.request.Request(  # noqa: S310
            f"http://127.0.0.1:{self.api}/api/v1/rules/test",
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.key}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as res:  # noqa: S310
                return res.status, json.load(res)
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode()[:500]
        except OSError as exc:
            return -1, f"request failed: {exc}"[:300]

    def api_call(self, method, path, body):
        """Run one management API call. Return (status, body or text)."""
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(  # noqa: S310
            f"http://127.0.0.1:{self.api}{path}",
            data=data,
            method=method,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.key}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as res:  # noqa: S310
                raw = res.read().decode()
                try:
                    return res.status, json.loads(raw)
                except ValueError:
                    return res.status, raw[:500]
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode()[:500]
            try:
                return exc.code, json.loads(raw)
            except ValueError:
                return exc.code, raw
        except OSError as exc:
            return -1, f"request failed: {exc}"[:300]

    def stop(self):
        """Stop the broker. Remove its directory."""
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None
        shutil.rmtree(self.tmp, ignore_errors=True)


def verify_rule_test(ex, broker):
    """Run every case of one rule example. Return (ok, record dict)."""
    if broker is None:
        return False, {"item": ex["item"], "error": "rule broker is missing"}
    ran = []
    for case in ex["cases"]:
        payload = json.loads(case["payload"])
        status, body = broker.dry_run(case["sql"], case["topic"], payload)
        if status != 200 or not isinstance(body, dict):
            return False, {
                "item": ex["item"],
                "error": f"rules/test gave {status}: {body}",
                "sql": case["sql"],
            }
        matched = body.get("matched")
        if matched != case["expect_matched"]:
            return False, {
                "item": ex["item"],
                "error": (
                    f"matched is {matched}, "
                    f"wanted {case['expect_matched']}"
                ),
                "sql": case["sql"],
            }
        if "expect_projected" in case:
            want = json.loads(case["expect_projected"])
            if body.get("projected") != want:
                return False, {
                    "item": ex["item"],
                    "error": (
                        f"projected is {body.get('projected')}, "
                        f"wanted {want}"
                    ),
                    "sql": case["sql"],
                }
        ran.append(
            {
                "sql": case["sql"],
                "topic": case["topic"],
                "payload": payload,
                "matched": matched,
                "projected": body.get("projected"),
            }
        )
    return True, {
        "item": ex["item"],
        "request": "POST /api/v1/rules/test",
        "returncode": 0,
        "cases": ran,
    }


# Rule function stems owned by DX2-002 (message context accessors).
DX2_002_ITEMS = {
    "fn-msgid",
    "fn-qos",
    "fn-flags",
    "fn-flag",
    "fn-topic",
    "fn-clientid",
    "fn-clientip",
    "fn-peerhost",
    "fn-username",
    "fn-payload",
    "fn-contains_topic",
    "fn-contains_topic_match",
    "fn-topic_match",
    "fn-null",
    "fn-coalesce_ne",
    "fn-div",
    "fn-fmod",
}


def failure_recorded(item):
    """Check whether failures.md already names this item."""
    try:
        text = FAILURES.read_text(encoding="utf-8")
    except OSError:
        return False
    return f"item `{item}`" in text


# Rule function stems owned by DX2-003 (math and bitwise).
DX2_003_ITEMS = {
    "fn-acosh",
    "fn-asinh",
    "fn-atanh",
    "fn-bitsl",
    "fn-bitsr",
}


# Rule function stems owned by DX2-004 (bit and byte).
DX2_004_ITEMS = {
    "fn-bitsize",
    "fn-bytesize",
    "fn-subbits",
}


# Rule function stems owned by DX2-005 (type conversion).
DX2_005_ITEMS = {
    "fn-bin2hexstr",
    "fn-bool",
    "fn-float",
    "fn-float2str",
    "fn-hexstr2bin",
    "fn-int",
    "fn-map",
    "fn-sqlserver_bin2hexstr",
    "fn-str",
    "fn-str_utf16_le",
    "fn-str_utf8",
}


# Rule function stems owned by DX2-006 (type checks).
DX2_006_ITEMS = {
    "fn-is_array",
    "fn-is_bool",
    "fn-is_empty",
    "fn-is_float",
    "fn-is_int",
    "fn-is_map",
    "fn-is_not_null",
    "fn-is_not_null_var",
    "fn-is_null",
    "fn-is_null_var",
    "fn-is_num",
    "fn-is_str",
}


# Rule function stems owned by DX2-009 (arrays).
DX2_009_ITEMS = {
    "fn-nth",
    "fn-length",
    "fn-sublist",
    "fn-first",
    "fn-last",
    "fn-contains",
    "fn-std_length",
}


# Rule function stems owned by DX2-010 (hashing).
DX2_010_ITEMS = {
    "fn-sha",
    "fn-hash",
    "fn-hash_to_range",
    "fn-map_to_range",
}


# Rule function stems owned by DX2-011 (compression).
DX2_011_ITEMS = {
    "fn-zip",
    "fn-unzip",
    "fn-gzip",
    "fn-gunzip",
    "fn-zip_compress",
    "fn-zip_uncompress",
    "fn-lz4_compress",
    "fn-lz4_uncompress",
}


# Rule function stems owned by DX2-014 (rule state).
DX2_014_ITEMS = {
    "fn-proc_dict_get",
    "fn-proc_dict_put",
    "fn-proc_dict_del",
    "fn-kv_store_get",
    "fn-kv_store_put",
    "fn-kv_store_del",
}


# Rule action stems owned by DX2-015 (actions A: alloydb to dynamodb).
DX2_015_ITEMS = {
    "action-alloydb",
    "action-aws_iot",
    "action-aws_timestream",
    "action-azure_blob",
    "action-azure_blob_storage",
    "action-azure_event_grid",
    "action-azure_event_hub_producer",
    "action-azure_eventhubs",
    "action-azure_iot",
    "action-bigquery",
    "action-cassandra",
    "action-clickhouse",
    "action-cockroachdb",
    "action-confluent",
    "action-confluent_producer",
    "action-console",
    "action-couchbase",
    "action-databricks",
    "action-datalayers",
    "action-disk_log",
    "action-doris",
    "action-dynamo",
    "action-dynamodb",
}


# Rule action stems owned by DX2-016 (actions B: elasticsearch to opentsdb).
DX2_016_ITEMS = {
    "action-elasticsearch",
    "action-gcp_iot",
    "action-gcp_pubsub",
    "action-gcp_pubsub_consumer",
    "action-gcp_pubsub_producer",
    "action-greptimedb",
    "action-http",
    "action-influxdb",
    "action-iotdb",
    "action-kafka",
    "action-kafka_consumer",
    "action-kafka_producer",
    "action-kinesis",
    "action-mongodb",
    "action-mqtt",
    "action-mqtt_bridge",
    "action-mssql",
    "action-mysql",
    "action-oci_streaming",
    "action-opc_ua",
    "action-opents",
    "action-opentsdb",
}


# Rule action stems owned by DX2-017 (actions C: oracle to webhook).
DX2_017_ITEMS = {
    "action-oracle",
    "action-pgsql",
    "action-postgres",
    "action-pulsar",
    "action-rabbitmq",
    "action-redis",
    "action-redshift",
    "action-rocketmq",
    "action-s3",
    "action-s3_tables",
    "action-s3tables",
    "action-snowflake",
    "action-snowflake_streaming",
    "action-sparkplug_b",
    "action-sqlserver",
    "action-tablestore",
    "action-tdengine",
    "action-test",
    "action-timescale",
    "action-timescaledb",
    "action-timestream",
    "action-webhook",
}


def verify_action_create(ex, broker):
    """Create one action through the management API. Return (ok, record)."""
    if broker is None:
        return False, {"item": ex["item"], "error": "rule broker is missing"}
    _, listed = broker.api_call("GET", "/api/v5/action_types", None)
    listed_names = listed if isinstance(listed, list) else []
    ran = []
    for case in ex["cases"]:
        name, kind = case["name"], case["type"]
        status, body = broker.api_call(
            "POST", "/api/v5/actions", {"type": kind, "name": name}
        )
        if status != case["expect_status"]:
            return False, {
                "item": ex["item"],
                "error": f"create {kind}/{name} gave {status}: {body}",
                "request": "POST /api/v5/actions",
            }
        if isinstance(body, dict):
            if "expect_type" in case and body.get("type") != case["expect_type"]:
                return False, {
                    "item": ex["item"],
                    "error": (
                        f"type is {body.get('type')}, "
                        f"wanted {case['expect_type']}"
                    ),
                    "request": "POST /api/v5/actions",
                }
            if "expect_code" in case and body.get("code") != case["expect_code"]:
                return False, {
                    "item": ex["item"],
                    "error": (
                        f"code is {body.get('code')}, "
                        f"wanted {case['expect_code']}"
                    ),
                    "request": "POST /api/v5/actions",
                }
        ran.append({"name": name, "type": kind, "status": status, "body": body})
    kinds = sorted({row["type"] for row in ran})
    return True, {
        "item": ex["item"],
        "request": "POST /api/v5/actions",
        "returncode": 0,
        "cases": ran,
        "action_types": kinds,
        "listed_in_action_types": all(k in listed_names for k in kinds),
    }


# Rule event stems owned by DX2-018 (event sources as FROM names).
DX2_018_ITEMS = {
    "event-events-client_check_authz_complete",
    "event-events-client_connack",
    "event-events-client_connected",
    "event-events-client_disconnected",
    "event-events-delivery_dropped",
    "event-events-message_acked",
    "event-events-message_delivered",
    "event-events-message_dropped",
    "event-events-message_publish",
    "event-events-no_such_event",
    "event-events-session_subscribed",
    "event-events-session_unsubscribed",
    "event-description",
    "event-event",
    "event-message_publish",
}


def failure_marker(item):
    """Name the wave task that owns one failure record."""
    if item.startswith(("clause-", "op-")):
        return "DX2-001"
    if item in DX2_002_ITEMS:
        return "DX2-002"
    if item in DX2_003_ITEMS:
        return "DX2-003"
    if item in DX2_004_ITEMS:
        return "DX2-004"
    if item in DX2_005_ITEMS:
        return "DX2-005"
    if item in DX2_006_ITEMS:
        return "DX2-006"
    if item in DX2_009_ITEMS:
        return "DX2-009"
    if item in DX2_010_ITEMS:
        return "DX2-010"
    if item in DX2_011_ITEMS:
        return "DX2-011"
    if item in DX2_014_ITEMS:
        return "DX2-014"
    if item in DX2_015_ITEMS:
        return "DX2-015"
    if item in DX2_016_ITEMS:
        return "DX2-016"
    if item in DX2_017_ITEMS:
        return "DX2-017"
    if item in DX2_018_ITEMS:
        return "DX2-018"
    return "DX-01"


def append_failure(item, ex, record):
    """Add one failure record. Never edit an example."""
    day = datetime.date.today().isoformat()
    task = failure_marker(item)
    if ex.get("kind") == "rule-test":
        inputs = "; ".join(
            f"rules/test `{c.get('sql', '')}` on `{c.get('topic', '')}`"
            for c in ex.get("cases", [])
        )
        block = (
            f"\n## {task} item `{item}`\n\n"
            f"- Input: {inputs}.\n"
            f"- Expected: the cased match results in the example.\n"
            f"- Broker gave: `{record.get('error', record)}`.\n"
            f"- Date: {day}.\n"
        )
    elif ex.get("kind") == "action-create":
        inputs = "; ".join(
            f"POST /api/v5/actions type `{c.get('type', '')}` "
            f"name `{c.get('name', '')}` wants {c.get('expect_status', '')}"
            for c in ex.get("cases", [])
        )
        block = (
            f"\n## {task} item `{item}`\n\n"
            f"- Input: {inputs}.\n"
            f"- Expected: the cased create results in the example.\n"
            f"- Broker gave: `{record.get('error', record)}`.\n"
            f"- Date: {day}.\n"
        )
    else:
        block = (
            f"\n## {task} item `{item}`\n\n"
            f"- Input: `{ex.get('binary', '')} {' '.join(ex.get('args', []))}`.\n"
            f"- Expected: exit 0 with `{ex.get('match', 'help text')}` in `--help`.\n"
            f"- Broker gave: `{record.get('error', record)}`.\n"
            f"- Date: {day}.\n"
        )
    with FAILURES.open("a", encoding="utf-8") as handle:
        handle.write(block)


# Qual mapping for connectors (DX2-019 through DX2-027).
# Each entry names the Rust qual test and its required env.
CONNECTOR_QUALS = {
    "couchbase": {
        "crate": "broker-connectors-enterprise",
        "test": "couchbase::tests::test_qualify_driver_write_path",
        "env": [
            "COUCHBASE_URL",
            "COUCHBASE_USERNAME",
            "COUCHBASE_PASSWORD",
            "COUCHBASE_BUCKET",
        ],
        "feature": "couchbase",
        "timeout": 2400,
        "markers": ["qual rows asserted", "qual expiry asserted"],
    },
    "gcp_iot": {
        "crate": "broker-connectors-enterprise",
        "test": "gcp_iot::tests::test_qualify_tls_write_path",
        "env": [
            "GCP_IOT_MQTT_URL",
            "GCP_IOT_PRIVATE_KEY_FILE",
        ],
        "feature": "gcp_iot",
        "timeout": 1800,
        "markers": ["qual rows asserted"],
    },
    "gcp_pubsub": {
        "crate": "broker-connectors-enterprise",
        "test": "gcp_pubsub::tests::test_qualify_driver_write_path",
        "env": [
            "PUBSUB_EMULATOR_HOST",
            "PUBSUB_PROJECT",
            "PUBSUB_TOPIC",
            "PUBSUB_SUBSCRIPTION",
        ],
        "feature": "gcp_pubsub",
        "timeout": 1800,
        "markers": ["qual rows asserted"],
    },
    "greptimedb": {
        "crate": "broker-connectors",
        "test": "greptimedb::tests::test_qualify_write_path",
        "env": [
            "GREPTIMEDB_ENDPOINT",
            "GREPTIMEDB_DATABASE",
            "GREPTIMEDB_TABLE",
        ],
        "timeout": 1800,
        "markers": ["qual rows asserted", "qual precision asserted"],
    },
    "influxdb": {
        "crate": "broker-connectors",
        "test": "influxdb::tests::test_qualify_write_path",
        "env": [
            "INFLUXDB_ENDPOINT",
            "INFLUXDB_ORG",
            "INFLUXDB_BUCKET",
            "INFLUXDB_TOKEN",
        ],
        "timeout": 1800,
        "markers": ["qual rows asserted", "qual auth failure asserted"],
    },
    "iotdb": {
        "crate": "broker-connectors-enterprise",
        "test": "iotdb::tests::test_qualify_write_path",
        "env": [
            "IOTDB_ENDPOINT",
            "IOTDB_USER",
            "IOTDB_PASSWORD",
            "IOTDB_DEVICE",
        ],
        "timeout": 2400,
        "markers": ["qual rows asserted", "qual fault asserted"],
    },
    "kafka": {
        "crate": "broker-connectors",
        "test": "kafka::tests::test_qualify_driver_write_path",
        "env": [
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_TOPIC",
        ],
        "feature": "kafka",
        "timeout": 1800,
        "markers": ["qual rows asserted"],
    },
    "mongodb": {
        "crate": "broker-connectors-enterprise",
        "test": "mongodb::tests::test_qualify_driver_write_path",
        "env": [
            "MONGODB_URL",
            "MONGODB_DATABASE",
            "MONGODB_COLLECTION",
        ],
        "feature": "mongodb",
        "timeout": 1800,
        "markers": ["qual rows asserted", "qual duplicate-key asserted"],
    },
    "mqtt_bridge": {
        "crate": "broker-connectors",
        "test": "mqtt_bridge::tests::test_qualify_bridge_write_path",
        "env": [
            "MQTT_BRIDGE_URL",
            "MQTT_BRIDGE_TOPIC",
            "MQTT_BRIDGE_CLIENT_ID",
        ],
        "timeout": 1800,
        "markers": ["qual rows asserted"],
    },
}

# Default timeout for the qual subprocess (secs).
# Each entry above overrides it with its ticket QUAL-TIMEOUT.
QUAL_TIMEOUT_SECS = 2400


def run_connectors_item(item, write):
    """Run one connectors qual test. Return exit code."""
    spec = CONNECTOR_QUALS.get(item)
    if spec is None:
        print(f"unknown connectors item `{item}`")
        return 1
    for name in spec["env"]:
        if not os.environ.get(name):
            raise SystemExit(
                f"qual {name} must be set for qualification; failing closed"
            )
    feature = spec.get("feature")
    timeout = int(spec.get("timeout", QUAL_TIMEOUT_SECS))
    cmd = [
        "cargo",
        "test",
        "-p",
        spec["crate"],
    ]
    # Kinds without a feature flag ship in the default build.
    # The command then carries no --features flag.
    if feature:
        cmd += ["--features", feature]
    cmd += [
        "--lib",
        spec["test"],
        "--",
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ]
    print(f"command: {' '.join(cmd)}")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"qual {item} timed out after {timeout} secs")
        return 1
    out = (proc.stdout + proc.stderr)[-20000:]
    print(out)
    if proc.returncode != 0:
        print(f"area=connectors items=1 verified=0 failed=1")
        return 1
    # An empty filter exits 0 with no test. Fail that run.
    # The empty run prints `0 passed; 0 failed`.
    if "0 passed; 0 failed" in out:
        print(f"area=connectors items=1 verified=0 failed=1")
        print("qual failed: filter matched 0 tests")
        return 1
    if "0 passed" in out and "1 passed" not in out:
        print(f"area=connectors items=1 verified=0 failed=1")
        print("qual failed: no test passed")
        return 1
    # The qual test proves the broker rule path. Require its markers.
    markers = spec.get("markers", ["qual rows asserted"])
    if any(m not in out for m in markers):
        print(f"area=connectors items=1 verified=0 failed=1")
        print("qual failed: broker rule path proof is missing")
        return 1
    if write:
        target = VERIFIED / "connectors" / (item + ".json")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(
                {
                    "item": item,
                    "command": cmd,
                    "returncode": 0,
                    "output": out[-4000:],
                    "verified": True,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    print(f"area=connectors items=1 verified=1 failed=0")
    return 0


def run_area(area, write, only_item=""):
    """Verify every example of one area. Print counts."""
    if area == "connectors":
        if only_item:
            return run_connectors_item(only_item, write)
        code = 0
        for name in sorted(CONNECTOR_QUALS):
            if run_connectors_item(name, write):
                code = 1
        return code
    items = verified = failed = 0
    area_dir = EXAMPLES / area
    if not area_dir.is_dir():
        print(f"area {area} has no examples")
        return 1
    broker = None
    if area == "rules":
        binary = find_binary("indramqtt")
        if binary is None:
            print("area=rules items=0 verified=0 failed=0")
            print("missing: rule broker binary is missing")
            return 1
        broker = RuleBroker(binary)
        try:
            broker.start()
        except SystemExit as exc:
            print(f"area=rules items=0 verified=0 failed=0\nbroker: {exc}")
            broker.stop()
            return 1
    try:
        for path in sorted(area_dir.glob("*.toml")):
            if only_item and path.stem != only_item:
                continue
            items += 1
            ex = load_example(path)
            if ex["kind"] == "rule-test":
                ok, record = verify_rule_test(ex, broker)
            elif ex["kind"] == "action-create":
                ok, record = verify_action_create(ex, broker)
            else:
                ok, record = verify_one(ex)
            target = VERIFIED / area / (path.stem + ".json")
            if ok:
                verified += 1
                if write:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    record["verified"] = True
                    target.write_text(
                        json.dumps(record, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
            else:
                failed += 1
                if write:
                    if target.is_file():
                        target.unlink()
                    if not failure_recorded(ex["item"]):
                        append_failure(ex["item"], ex, record)
                else:
                    print(f"failed: {ex['item']}: {record.get('error', record)}")
        print(f"area={area} items={items} verified={verified} failed={failed}")
    finally:
        if broker is not None:
            broker.stop()
    # Fail when an item has neither a verified file nor a record.
    missing = 0
    for path in sorted(area_dir.glob("*.toml")):
        if only_item and path.stem != only_item:
            continue
        ex = load_example(path)
        target = VERIFIED / area / (path.stem + ".json")
        has_file = target.is_file() if not write else None
        if write:
            has_file = target.is_file()
        if not has_file and not failure_recorded(ex["item"]):
            print(f"missing: {ex['item']} has no verified file or record")
            missing += 1
    if only_item and items == 0:
        print(f"unknown item `{only_item}` in area `{area}`")
        return 1
    return 1 if missing else 0


def main(argv):
    """Run the verifier. --write stores verified files."""
    parser = argparse.ArgumentParser(description="Verify doc examples.")
    parser.add_argument("--area", default="", help="one area only")
    parser.add_argument("--item", default="", help="one item only")
    parser.add_argument("--all", action="store_true", help="every area")
    parser.add_argument("--write", action="store_true", help="store results")
    args = parser.parse_args(argv)
    if args.all:
        areas = sorted(p.name for p in EXAMPLES.iterdir() if p.is_dir())
    elif args.area:
        areas = [args.area]
    else:
        parser.print_help()
        return 2
    code = 0
    for area in areas:
        if run_area(area, args.write, args.item):
            code = 1
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
