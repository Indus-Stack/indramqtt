"""Check typed inventory coverage and the audit state of each row."""
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
ENTRY_PREFIXES = ("tests/settings_inventory/entry.py", "tests/e2e_integration.rs")
NO_EFFECT = (
    "state.connectors.connectors[type=console].config",
    "state.connectors.connectors[type=test].config",
)
STATES = {"entry evidence", "evidence pending", "no runtime effect"}
SECRET_SEGMENTS = {
    "password", "passwd", "secret", "token", "api_key", "apikey", "access_key",
    "access_key_id", "access_key_secret", "account_key", "accountkey",
    "private_key", "private_key_pem", "key_pem", "keyfile", "key_password",
    "acc_key", "auth_token", "client_key", "shared_access_key", "mfa_secret",
    "key_hash", "bind_password", "password_hash", "session_token", "sas_token",
    "bearer_token", "secret_key", "secret_access_key", "api_secret",
}


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
                "evidence", "credential", "risk", "proposal", "owner", "legacy", "status",
                "definition", "type"}
    for row in rows:
        if required - row.keys():
            raise AssertionError("incomplete row columns: " + row["field"])
        if any(not isinstance(row[key], str) or not row[key].strip() for key in required):
            raise AssertionError("empty row column: " + row["field"])
        owner = row["owner"]
        if not (re.fullmatch(r"(?:AZ|CX|KC|MV|RO)-[0-9]{2}[a-z]?", owner)
                or re.fullmatch(r"new: [^;|\n]+", owner)):
            raise AssertionError("field must have one ticket or one new-ticket title: " + row["field"])
        if owner == "KC-00":
            raise AssertionError("per-field row must not use the kind-map owner: " + row["field"])
        state = row["status"]
        if state not in STATES:
            raise AssertionError("unknown audit state: " + row["field"] + ": " + state)
        evidence = row["evidence"]
        if state == "entry evidence" and not evidence.startswith(ENTRY_PREFIXES):
            raise AssertionError("entry evidence does not cite an entry test: " + row["field"])
        if state == "no runtime effect" and not (
                evidence.startswith("Pure metadata") or row["field"] in NO_EFFECT):
            raise AssertionError("no-runtime-effect row has no stated reason: " + row["field"])
        if state == "evidence pending" and evidence.startswith(ENTRY_PREFIXES):
            raise AssertionError("entry evidence is marked pending: " + row["field"])
        if row["credential"] == "n/a" and any(
                part in SECRET_SEGMENTS for part in row["field"].split(".")):
            raise AssertionError("credential row has no exposure record: " + row["field"])
    if "state.reload_import.atomicity" not in names:
        raise AssertionError("the cross-root atomicity row is missing")
    pending = [row["field"] for row in rows if row["status"] == "evidence pending"]
    if ledger["status"] == "complete" and pending:
        raise AssertionError("complete inventory still has pending evidence rows")
    counts = {state: sum(row["status"] == state for row in rows) for state in sorted(STATES)}
    print(str(len(expected)) + " typed fields covered; " + str(len(rows)) + " total rows.")
    print("Audit state: " + ", ".join(state + "=" + str(count) for state, count in counts.items()))
    print("This checks coverage and audit state, not runtime correctness.")


if __name__ == "__main__":
    check()
