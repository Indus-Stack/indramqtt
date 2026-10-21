"""Render the field ledger without turning pending evidence into completion."""
from collections import Counter
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "docs/reference/settings-inventory.json"
TARGET = ROOT / "docs/reference/unapplied-settings.md"
KEYS = ["field", "store", "reader", "condition", "restart", "expectation", "current",
        "evidence", "risk", "proposal", "owner", "legacy", "status"]


def cell(value):
    return str(value).replace("|", " / ").replace("\n", " ")


def render(ledger):
    rows = ledger["rows"]
    names = [row["field"] for row in rows]
    if len(set(names)) != len(names):
        raise ValueError("duplicate inventory field")
    counts = Counter(row["status"] for row in rows)
    if ledger["status"] == "complete":
        intro = (
            "This page lists every stored setting. Use it when a setting is "
            "accepted but its effect is not clear. Each row gives the runtime "
            "reader, the current behavior, the risk and one owner ticket. This "
            "audit does not fix any defect.\n\n"
        )
        coverage = (
            str(len(rows)) + " rows. Every row is reviewed and has one owner. "
            "A complete inventory does not mean that a stored setting works.\n\n"
        )
    else:
        intro = (
            "This audit is in progress. Pending evidence is not a statement that "
            "the setting works or that there is no reader. No production defect is "
            "fixed here.\n\n"
        )
        coverage = (
            str(len(rows)) + " rows. Opaque field expansion and evidence review are "
            "still in progress.\n\n"
        )
    text = "# Stored setting audit\n\n" + intro
    text += """```mermaid
flowchart LR
    F["Startup file<br/>file.*"] --> S["Config registry<br/>data_dir/state.toml"]
    A["Management API<br/>api.*"] --> S
    M["MCP tools/call"] --> A
    O["Old state.toml records"] --> S
    S --> R{"Runtime reader?"}
    R -->|yes| B["Broker action<br/>socket, message or API"]
    R -->|no| G["Gap with one owner ticket"]
```

`file.` identifies a startup setting. `state.` identifies a leaf in
`data_dir/state.toml`. `api.` identifies other accepted management input.
Arrays use `[]`; maps use `{key}`. The JSON ledger beside this file is the
source of this table. Source line numbers refer to base
`""" + ledger["base"] + """`. Short source names are within `crates/`; `main.rs`
is `broker-node/src/main.rs`.

The entry cases are in `tests/settings_inventory/entry.py`. They start a real
broker process, write through its file or HTTP entry, then use MQTT sockets
or HTTP. Temporary defect assertions carry an owner marker. A passing case
records current behavior. Its owner must replace that assertion. Backend
construction traces and entry evidence are recorded separately.

For old unsupported security state, D2 requires a denial at the affected
source, an alarm, preservation of the raw record and management read/delete.
A supported earlier decision remains valid. A later allow cannot bypass the
unsupported source. New unsupported values must be refused before storage.

## Coverage state

"""
    text += coverage
    text += "| Audit state | Rows |\n| --- | ---: |\n"
    for state, count in sorted(counts.items()):
        text += "| " + cell(state) + " | " + str(count) + " |\n"
    text += "\n## Shared entry points\n\n| Entry | Stored field family | Notes |\n| --- | --- | --- |\n"
    for alias in ledger.get("entry_aliases", []):
        text += "| " + " | ".join(cell(alias[key]) for key in ["entry", "state", "notes"]) + " |\n"
    for label, prefix in [("Startup fields", "file."), ("Persisted fields", "state."),
                          ("Other accepted fields", "api.")]:
        text += "\n## " + label + "\n\n"
        text += "| Field / definition | Store | Runtime reader | Build condition | Restart | User expectation | Current behavior | Evidence | Risk | Proposal | Owner | Old state / safe startup | Audit |\n"
        text += "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
        for row in rows:
            if not row["field"].startswith(prefix):
                continue
            values = [row[key] for key in KEYS]
            values[0] = "`" + row["field"] + "` (" + row["definition"] + ")"
            text += "| " + " | ".join(cell(value) for value in values) + " |\n"
    return text


if __name__ == "__main__":
    content = render(json.loads(SOURCE.read_text()))
    if sys.argv[1:] == ["--check"]:
        if not TARGET.exists() or TARGET.read_text() != content:
            raise SystemExit("inventory table differs from its ledger; run render.py")
    elif sys.argv[1:]:
        raise SystemExit("usage: render.py [--check]")
    else:
        TARGET.write_text(content)
