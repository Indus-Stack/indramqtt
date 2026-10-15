#!/usr/bin/env python3
"""Run doc examples through the real entry. Capture output (DX-01).

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
  python3 docs/_tools/verify.py --all [--write]
Without --write, check only. It prints items, verified,
failed. It fails when an item has no verified file and
no failure record.
"""
import argparse
import datetime
import json
import os
import subprocess
import sys
import tempfile
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
    for key in ("area", "item", "kind", "binary", "args"):
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


def failure_recorded(item):
    """Check whether failures.md already names this item."""
    try:
        text = FAILURES.read_text(encoding="utf-8")
    except OSError:
        return False
    return f"DX-01 item `{item}`" in text


def append_failure(item, ex, record):
    """Add one failure record. Never edit an example."""
    day = datetime.date.today().isoformat()
    block = (
        f"\n## DX-01 item `{item}`\n\n"
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
    for path in sorted(area_dir.glob("*.toml")):
        items += 1
        ex = load_example(path)
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
    print(f"area={area} items={items} verified={verified} failed={failed}")
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
