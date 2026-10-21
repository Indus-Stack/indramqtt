"""Check typed inventory coverage; runtime evidence still needs review."""
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]


def structs_from(path):
    lines = path.read_text().splitlines()
    structs = {}
    name = None
    for line in lines:
        match = re.match(r"pub struct (\w+)\s*\{", line)
        if match:
            name = match.group(1)
            structs[name] = []
        elif name and line.startswith("}"):
            name = None
        elif name:
            field = re.match(r"\s+pub(?:\([^)]*\))?\s+(\w+):\s*(.+),\s*$", line)
            if field:
                structs[name].append(field.groups())
            elif re.match(r"\s+pub(?:\([^)]*\))?\s+\w+:", line):
                raise ValueError("field format needs explicit parser review: " + name + ": " + line.strip())
    return structs


def typed_fields(structs, name, prefix, stack=()):
    if name in stack:
        raise ValueError("recursive inventory struct: " + name)
    result = set()
    for field, type_name in structs[name]:
        path = prefix + "." + field
        vector = re.fullmatch(r"Vec<(.+)>", type_name)
        optional = re.fullmatch(r"Option<(.+)>", type_name)
        inner = vector.group(1) if vector else optional.group(1) if optional else type_name
        if inner in structs:
            result.update(typed_fields(structs, inner, path + ("[]" if vector else ""), stack + (name,)))
        else:
            result.add(path)
    return result


def check():
    structs = {}
    for name in ["schema.rs", "lib.rs"]:
        structs.update(structs_from(ROOT / "crates/broker-config/src" / name))
    expected = typed_fields(structs, "BrokerConfig", "file")
    expected.update(typed_fields(structs, "FullSnapshot", "state"))
    ledger = json.loads((ROOT / "docs/reference/settings-inventory.json").read_text())
    rows = ledger["rows"]
    names = [row["field"] for row in rows]
    if len(names) != len(set(names)):
        raise AssertionError("inventory has duplicate field rows")
    missing = expected - set(names)
    if missing:
        raise AssertionError("typed fields missing from inventory: " + ", ".join(sorted(missing)))
    required = {"field", "store", "reader", "condition", "restart", "expectation", "current",
                "evidence", "risk", "proposal", "owner", "legacy", "status", "definition", "type"}
    for row in rows:
        if required - row.keys():
            raise AssertionError("incomplete row columns: " + row["field"])
        if any(not isinstance(row[key], str) or not row[key].strip() for key in required):
            raise AssertionError("empty row column: " + row["field"])
        owner = row["owner"]
        if not (re.fullmatch(r"(?:AZ|CX|KC|MV|RO)-[0-9]{2}[a-z]?", owner)
                or re.fullmatch(r"new: [^;|\n]+", owner)):
            raise AssertionError("field must have one ticket or one new-ticket title: " + row["field"])
    if ledger["status"] == "complete":
        unfinished = [row["field"] for row in rows if row["status"] != "reviewed"]
        if unfinished:
            raise AssertionError("complete inventory still has unfinished rows: " + ", ".join(unfinished))
    print(str(len(expected)) + " typed fields covered; " + str(len(rows)) + " total rows.")
    print("This checks coverage, not runtime correctness or audit completion.")


if __name__ == "__main__":
    check()
