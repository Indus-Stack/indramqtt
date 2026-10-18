#!/usr/bin/env python3
"""Build the product inventories from the code (DX-01).

Read the sources. Write JSON. Keep the two in sync.
Run from the workspace root:
  python3 docs/_tools/inventory.py --write   # rewrite the files
  python3 docs/_tools/inventory.py --check   # fail when a file drifts

One generator owns all seven files in docs/_inventory/:
  cli.json, config.json, api.json, mcp.json,
  rules.json, connectors.json, metrics.json.
A drift test fails when a committed file differs.
Standard library only. No network.
"""
import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs" / "_inventory"
TOOLS = ROOT / "tools"

sys.path.insert(0, str(TOOLS))
import gen_reference_docs as gen  # noqa: E402


def write_json(name, payload):
    """Write one inventory file with stable key order."""
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    path.write_text(text, encoding="utf-8")


def read_json(name):
    """Read one committed inventory file."""
    return json.loads((OUT / name).read_text(encoding="utf-8"))


def build_cli():
    """List every binary, command and flag from the code."""
    # The tables are the canonical list. The check below
    # proves each row against the command definitions.
    gen.check_cli_against_sources()
    flags = [
        {
            "flag": flag,
            "kind": kind,
            "default": default,
            "setting": home,
            "effect": effect,
        }
        for flag, kind, default, home, effect in gen.INDRA_MQTT_FLAGS
    ]
    commands = [
        {"command": "indra " + name, "effect": effect}
        for name, effect in gen.CTL_COMMANDS
    ]
    # Run each --help when the binaries exist. The gates build
    # first, so the check sees live help there. A missing binary
    # keeps the source parse as the inventory with a clear state.
    # The live --help run depends on a built binary in ./target, which
    # exists on some machines and not on others. Run it only on request
    # (INVENTORY_LIVE_HELP=1), so the committed inventory is the same
    # everywhere and the drift check does not depend on the machine.
    if os.environ.get("INVENTORY_LIVE_HELP") == "1":
        live = try_live_help()
    else:
        live = {"binaries": {}, "checked": False, "state": "not-run"}
    return {
        "binaries": [
            {"name": "indramqtt", "flags": flags},
            {"name": "indra", "commands": commands},
        ],
        "live_help": live,
    }


def try_live_help():
    """Run --help on each binary when one exists. No build here."""
    state = {"checked": False, "binaries": {}}
    cands = [
        ROOT / "target" / "debug" / "indramqtt",
        ROOT / "target" / "ci-check" / "debug" / "indramqtt",
    ]
    kernel = next((p for p in cands if p.is_file()), None)
    ctl = next(
        (
            p
            for p in (
                ROOT / "target" / "debug" / "indra",
                ROOT / "target" / "ci-check" / "debug" / "indra",
            )
            if p.is_file()
        ),
        None,
    )
    if kernel is None and ctl is None:
        state["state"] = "binary-missing"
        return state
    state["checked"] = True
    state["state"] = "checked"
    if kernel is not None:
        state["binaries"]["indramqtt"] = run_help([str(kernel)])
    if ctl is not None:
        state["binaries"]["indra"] = run_help([str(ctl), "ctl", "--help"])
        # TODO(parity): which ctl subgroup help lines form the full
        # command list when a group gains a nested level? The rulebook
        # does not decide the depth. Current choice records top help.
    return state


def run_help(argv):
    """Run one --help. Return its flags and exit code."""
    try:
        proc = subprocess.run(
            argv + ["--help"] if argv[-1] != "--help" else argv,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)[:120]}
    text = proc.stdout + proc.stderr
    flags = sorted({m for m in re.findall(r"--[a-z][a-z0-9-]*", text)})
    return {"ok": proc.returncode == 0, "flags": flags}


def build_config():
    """List every setting of the configuration schema."""
    fields = gen.load_fields()
    return {
        "settings": [
            {
                "path": f["path"],
                "type": f["type"],
                "default": f["default"],
                "range": f["range"],
                "doc": f["doc"],
            }
            for f in fields
        ]
    }


def build_api():
    """List every operation of the served OpenAPI document."""
    spec = gen.load_openapi()
    ops = []
    for path in sorted(spec.get("paths", {})):
        item = spec["paths"][path]
        if not isinstance(item, dict):
            continue
        for method in sorted(item):
            op = item[method]
            if not isinstance(op, dict):
                continue
            tags = op.get("tags", [])
            ops.append(
                {
                    "method": method,
                    "path": path,
                    "operation_id": op.get("operationId", ""),
                    "tags": tags if isinstance(tags, list) else [],
                    "summary": op.get("summary", ""),
                }
            )
    return {"operations": ops}


def build_mcp():
    """List every tool that /mcp lists. Empty: no MCP endpoint."""
    # The tree serves no MCP endpoint today. The page docs/mcp.md
    # states the plan. A grep over the API crate proves the absence.
    hits = []
    api = ROOT / "crates" / "broker-api" / "src"
    for path in api.rglob("*.rs"):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        if "/mcp" in text:
            hits.append(str(path.relative_to(ROOT)))
    return {"tools": [], "note": "Tree has no MCP endpoint.", "hits": hits}


def rust_string_list(src, const_name):
    """Read one &[&str] constant body from Rust source."""
    match = re.search(
        const_name + r"\s*:\s*&\[[^\]]*?\]?\s*=\s*&\[(.*?)\];",
        src,
        re.DOTALL,
    )
    if match is None:
        return []
    return re.findall(r'"([^"]+)"', match.group(1))


def build_rules():
    """List SQL clauses, operators, functions, actions and events."""
    rules = ROOT / "crates" / "broker-rules" / "src"
    stdlib = (rules / "stdlib.rs").read_text(encoding="utf-8")
    # Broker UDF names registered in stdlib.rs. Each registration
    # names the function once. Sorted for a stable file.
    udfs = sorted(set(re.findall(r'add\("([a-zA-Z_][a-zA-Z0-9_]*)"', stdlib)))
    events_src = (rules / "events.rs").read_text(encoding="utf-8")
    events = sorted(set(re.findall(r'"([a-zA-Z0-9_/+#$-]+)"', events_src)))[:64]
    std_sql = (rules / "std_sql.rs").read_text(encoding="utf-8")
    clauses = sorted(
        set(re.findall(r"\b(SELECT|FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT)\b", std_sql))
    )
    operators = sorted(
        set(
            re.findall(
                r"\b(AND|OR|NOT|LIKE|IN|BETWEEN|IS NULL|IS NOT NULL)\b", std_sql
            )
        )
    )
    connectors = build_connectors()
    actions = sorted({c["kind"] for c in connectors["connectors"]})
    return {
        "clauses": clauses,
        "operators": operators,
        # TODO(parity): which builtin names does the SQL engine crate
        # export at runtime beyond these tree UDFs? The rulebook does
        # not decide the static source. Current choice lists tree UDFs.
        "functions": [{"name": name} for name in udfs],
        "builtin_source": "rekuiper_sql::builtin_function_metadata",
        "actions": actions,
        "events": events,
    }


def build_connectors():
    """List every connector kind and its tier from the code."""
    lib = (ROOT / "crates" / "broker-config" / "src" / "lib.rs").read_text(
        encoding="utf-8"
    )
    known = rust_string_list(lib, "KNOWN_CONNECTOR_TYPES")
    actions = rust_string_list(lib, "KNOWN_ACTION_TYPES")
    conn_lib = (ROOT / "crates" / "broker-connectors" / "src" / "lib.rs").read_text(
        encoding="utf-8"
    )
    tiers = dict(re.findall(r'"([a-z0-9_]+)"\s*=>\s*"(community|enterprise)"', conn_lib))
    kinds = sorted(set(known) | set(actions))
    return {
        "connectors": [
            {"kind": k, "tier": tiers.get(k, "community")} for k in kinds
        ]
    }


def build_metrics():
    """List every metric name from the live metrics endpoint code."""
    src = (ROOT / "crates" / "broker-observability" / "src" / "lib.rs").read_text(
        encoding="utf-8"
    )
    names = sorted(set(re.findall(r"indramqtt_[a-z0-9_]+", src)))
    return {"metrics": [{"name": n} for n in names]}


BUILDERS = {
    "cli.json": build_cli,
    "config.json": build_config,
    "api.json": build_api,
    "mcp.json": build_mcp,
    "rules.json": build_rules,
    "connectors.json": build_connectors,
    "metrics.json": build_metrics,
}


def build_all():
    """Build every inventory payload. Return name to payload."""
    return {name: fn() for name, fn in BUILDERS.items()}


def main(argv):
    """Run the generator. --write stores. --check diffs."""
    parser = argparse.ArgumentParser(description="Build product inventories.")
    parser.add_argument("--write", action="store_true", help="rewrite the files")
    parser.add_argument("--check", action="store_true", help="fail on drift")
    args = parser.parse_args(argv)
    fresh = build_all()
    if args.write:
        for name, payload in fresh.items():
            write_json(name, payload)
        print(f"wrote {len(fresh)} inventories to {OUT}")
        return 0
    if args.check:
        bad = 0
        for name, payload in fresh.items():
            want = json.dumps(payload, indent=2, sort_keys=True) + "\n"
            try:
                have = (OUT / name).read_text(encoding="utf-8")
            except OSError:
                print(f"drift: {name} is missing: run --write")
                bad += 1
                continue
            if have != want:
                print(f"drift: {name} differs: run --write")
                bad += 1
        if bad:
            return 1
        print(f"inventories match: {len(fresh)} files")
        return 0
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
