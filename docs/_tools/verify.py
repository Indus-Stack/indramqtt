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


def run_area(area, write):
    """Verify every example of one area. Print counts."""
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
            items += 1
            ex = load_example(path)
            if ex["kind"] == "rule-test":
                ok, record = verify_rule_test(ex, broker)
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
        ex = load_example(path)
        target = VERIFIED / area / (path.stem + ".json")
        has_file = target.is_file() if not write else None
        if write:
            has_file = target.is_file()
        if not has_file and not failure_recorded(ex["item"]):
            print(f"missing: {ex['item']} has no verified file or record")
            missing += 1
    return 1 if missing else 0


def main(argv):
    """Run the verifier. --write stores verified files."""
    parser = argparse.ArgumentParser(description="Verify doc examples.")
    parser.add_argument("--area", default="", help="one area only")
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
        if run_area(area, args.write):
            code = 1
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
