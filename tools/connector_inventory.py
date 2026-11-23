#!/usr/bin/env python3
"""Generate the connector inventory and report drift.

Read the connector source in both connector crates. Write one record
for each connector kind: its name, direction, status and configuration
items. Run with --write to update the committed files. Run with --check
to fail when the committed files differ from the source.
"""

import argparse
import difflib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "tests" / "e2e" / "connectors"
JSON_FILE = OUT_DIR / "inventory.json"
MARKDOWN_FILE = OUT_DIR / "INVENTORY.md"

CRATES = [
    ("broker-connectors", ROOT / "crates" / "broker-connectors" / "src"),
    ("broker-connectors-enterprise", ROOT / "crates" / "broker-connectors-enterprise" / "src"),
]

OUT_OF_MVP = {"aws_iot", "azure_iot", "oci_streaming"}

# Kinds whose code carries an inbound and an outbound path.
BIDIRECTIONAL = {"aws_iot", "azure_iot", "gcp_iot", "opc_ua", "sparkplug_b"}

SKIP_FILES = {"lib.rs", "cloud_tls.rs", "pg_numbers.rs"}

CATEGORY_RULES = [
    ("tls", ("tls", "ssl", "cert", "ca_", "https")),
    ("auth", ("auth", "username", "password", "token", "credential", "secret",
              "signature", "hmac", "api_key", "apikey", "sas", "access_key")),
    ("batching", ("batch", "linger", "flush")),
    ("retry", ("retr", "backoff", "timeout", "keep_alive", "keepalive", "reconnect")),
    ("buffering", ("buffer", "capacity", "inflight", "queue", "window")),
    ("mapping", ("template", "mapping", "prefix", "header", "field", "column", "key",
                  "client_id", "protocol", "format", "method", "qos", "retain")),
    ("endpoint", ("url", "address", "endpoint", "server", "host", "port", "path",
                  "region", "bucket", "table", "database", "collection", "project",
                  "namespace", "topic", "container", "cluster")),
]

MANUAL_CONNECTORS = [
    {
        "kind": "console",
        "crate": "broker-connectors",
        "module": "lib",
        "source": "crates/broker-connectors/src/lib.rs",
        "direction": "sink",
        "config": "none",
        "items": [],
    }
]


def strip_comments(text):
    """Remove Rust line comments and block comments, keeping strings."""
    out = []
    i = 0
    n = len(text)
    state = None
    while i < n:
        c = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if state is None:
            if c == "/" and nxt == "/":
                state = "line"
                i += 2
                continue
            if c == "/" and nxt == "*":
                state = "block"
                i += 2
                continue
            if c == '"':
                state = "string"
            out.append(c)
        elif state == "line":
            if c == "\n":
                state = None
                out.append(c)
        elif state == "block":
            if c == "*" and nxt == "/":
                state = None
                i += 2
                continue
        elif state == "string":
            out.append(c)
            if c == "\\":
                if nxt:
                    out.append(nxt)
                    i += 2
                    continue
            elif c == '"':
                state = None
        i += 1
    return "".join(out)


def matching_brace(text, start):
    """Return the closing brace index for the brace at start."""
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return i
    raise ValueError("unbalanced braces")


def find_types(text):
    """Find local structs and enums, with their bodies."""
    structs = {}
    enums = {}
    for match in re.finditer(r"pub\s+struct\s+(\w+)\s*(?:<[^>{}]*>)?\s*\{", text):
        start = text.find("{", match.start())
        end = matching_brace(text, start)
        structs[match.group(1)] = text[start + 1:end]
    for match in re.finditer(r"pub\s+enum\s+(\w+)\s*(?:<[^>{}]*>)?\s*\{", text):
        start = text.find("{", match.start())
        end = matching_brace(text, start)
        enums[match.group(1)] = text[start + 1:end]
    return structs, enums


def split_top_level(body, delimiter=","):
    parts = []
    depth = 0
    current = []
    for c in body:
        if c in "([{<":
            depth += 1
        elif c in ")]}>":
            depth -= 1
        if c == delimiter and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(c)
    if "".join(current).strip():
        parts.append("".join(current))
    return parts


def parse_fields(body):
    fields = []
    for chunk in split_top_level(body):
        chunk = re.sub(r"#\[[^\]]*\]", " ", chunk)
        chunk = " ".join(line for line in chunk.splitlines() if not line.strip().startswith("///"))
        chunk = re.sub(r"\s+", " ", chunk).strip()
        match = re.match(r"pub(?:\s*\([^)]*\))?\s+(\w+)\s*:\s*(.+?)\s*$", chunk)
        if match:
            name, type_name = match.groups()
            default = re.search(r'serde\(default\s*=\s*"([^"]+)"\)', chunk)
            fields.append({
                "field": name,
                "type": re.sub(r"\s+", " ", type_name).strip(),
                "default": default.group(1) if default else None,
            })
    return fields


def parse_variants(body):
    variants = []
    for chunk in split_top_level(body):
        chunk = re.sub(r"#\[[^\]]*\]", " ", chunk)
        chunk = re.sub(r"///[^\n]*", " ", chunk)
        chunk = re.sub(r"\s+", " ", chunk).strip()
        match = re.match(r"(\w+)", chunk)
        if match and match.group(1) not in {"pub"}:
            variants.append(match.group(1))
    return variants


def inner_type(type_name):
    value = type_name.strip()
    changed = True
    while changed:
        changed = False
        for prefix in ("Option<", "Vec<", "Box<", "Arc<"):
            if value.startswith(prefix) and value.endswith(">"):
                value = value[len(prefix):-1].strip()
                changed = True
    return value


def category_for(field, type_name):
    name = field.lower()
    kind_type = type_name.lower()
    for category, words in CATEGORY_RULES:
        if any(word in name for word in words):
            return category
    if "tls" in kind_type or "ssl" in kind_type:
        return "tls"
    if "config" in kind_type or "auth" in kind_type:
        return "other"
    return "other"


def item_id(kind, path):
    return kind + "." + path


def build_items(kind, config_name, structs, enums, file_text):
    items = []
    seen = set()

    def add(path, field, type_name, category, origin):
        ident = item_id(kind, path)
        if ident in seen:
            return
        seen.add(ident)
        items.append({
            "id": ident,
            "field": field,
            "path": path,
            "type": type_name,
            "category": category,
            "origin": origin,
        })

    def walk(struct_name, prefix, stack):
        if struct_name in stack or struct_name not in structs:
            return
        for entry in parse_fields(structs[struct_name]):
            field = entry["field"]
            type_name = entry["type"]
            path = prefix + "." + field if prefix else field
            category = category_for(field, type_name)
            add(path, field, type_name, category, "config")
            base = inner_type(type_name)
            if base in enums:
                for variant in parse_variants(enums[base]):
                    add(path + "." + variant, variant, base,
                        category, "variant")
            if base in structs:
                walk(base, path, stack + (struct_name,))

    walk(config_name, "", ())

    if re.search(r"(?i)\btls\b|https", file_text) or kind in {"mqtt_bridge"}:
        exists = any(item["category"] == "tls" for item in items)
        if not exists:
            add("tls", "tls", "feature", "tls", "source")

    return items


def config_type_for(text, structs):
    candidates = []
    for match in re.finditer(r"config\s*:\s*([A-Za-z_][A-Za-z0-9_]*)", text):
        name = match.group(1)
        if name in structs and name.endswith("Config"):
            candidates.append(name)
    for match in re.finditer(r"fn\s+new\s*\([^)]*config\s*:\s*([A-Za-z_][A-Za-z0-9_]*)", text, re.S):
        name = match.group(1)
        if name in structs and name.endswith("Config"):
            candidates.append(name)
    if candidates:
        return max(candidates, key=candidates.count)
    for name in structs:
        if name.endswith("Config"):
            return name
    return None


def connector_records():
    records = []
    global_enums = {}
    for _, directory in CRATES:
        for path in sorted(directory.glob("*.rs")):
            if path.name in SKIP_FILES:
                continue
            _, enums = find_types(strip_comments(path.read_text()))
            for name, body in enums.items():
                global_enums.setdefault(name, body)
    for crate, directory in CRATES:
        for path in sorted(directory.glob("*.rs")):
            if path.name in SKIP_FILES:
                continue
            text = strip_comments(path.read_text())
            kind_match = re.search(
                r"fn\s+kind\s*\(\s*&self\s*\)\s*->\s*&'static\s+str\s*\{\s*\"([^\"]+)\"",
                text,
            )
            if kind_match:
                kind = kind_match.group(1)
            else:
                method = re.search(
                    r"fn\s+kind\s*\(\s*&self\s*\)\s*->\s*&'static\s+str",
                    text,
                )
                field_match = re.search(r"\bkind\s*:\s*\"([^\"]+)\"", text)
                if not method or not field_match:
                    continue
                kind = field_match.group(1)
            structs, local_enums = find_types(text)
            enums = dict(local_enums)
            for name, body in global_enums.items():
                if name not in structs:
                    enums.setdefault(name, body)
            config_name = config_type_for(text, structs)
            if not config_name:
                continue
            source = path.relative_to(ROOT).as_posix()
            items = build_items(kind, config_name, structs, enums, text)
            records.append({
                "kind": kind,
                "crate": crate,
                "module": path.stem,
                "source": source,
                "direction": "both" if kind in BIDIRECTIONAL else "sink",
                "config": config_name,
                "items": items,
            })
    for manual in MANUAL_CONNECTORS:
        records.append(dict(manual))
    for record in records:
        record["status"] = "out-of-mvp" if record["kind"] in OUT_OF_MVP else "mvp"
    records.sort(key=lambda record: record["kind"])
    return records


def render_json(records):
    document = {
        "generated_by": "tools/connector_inventory.py",
        "connector_count": len(records),
        "item_count": sum(len(record["items"]) for record in records),
        "connectors": records,
    }
    return json.dumps(document, indent=2, sort_keys=False) + "\n"


def render_markdown(records):
    lines = [
        "# Connector end-to-end inventory",
        "",
        "This file is generated from the connector source.",
        "Run `python3 tools/connector_inventory.py --write` to update it.",
        "The drift test fails when this file differs from the source.",
        "",
        f"Connectors: {len(records)}.",
        f"Items: {sum(len(record['items']) for record in records)}.",
        "",
        "| Kind | Crate | Direction | Status | Config type | Items |",
        "|---|---|---|---|---|---|",
    ]
    for record in records:
        lines.append(
            "| `{kind}` | {crate} | {direction} | {status} | `{config}` | {items} |".format(
                kind=record["kind"],
                crate=record["crate"],
                direction=record["direction"],
                status=record["status"],
                config=record["config"],
                items=len(record["items"]),
            )
        )
    lines.append("")
    for record in records:
        lines.extend([
            f"## `{record['kind']}`",
            "",
            f"Source: `{record['source']}`.",
            f"Direction: {record['direction']}. Status: {record['status']}.",
            "",
        ])
        if not record["items"]:
            lines.extend(["No configuration fields.", ""])
            continue
        lines.extend([
            "| Item | Field | Type | Category |",
            "|---|---|---|---|",
        ])
        for item in record["items"]:
            type_name = item["type"].replace("|", "\\|")
            lines.append(
                f"| `{item['id']}` | `{item['field']}` | `{type_name}` | {item['category']} |"
            )
        lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="write the committed files")
    parser.add_argument("--check", action="store_true", help="fail when the files drift")
    args = parser.parse_args()
    records = connector_records()
    outputs = {
        JSON_FILE: render_json(records),
        MARKDOWN_FILE: render_markdown(records),
    }
    if args.write:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        for path, text in outputs.items():
            path.write_text(text)
        print(f"wrote {len(records)} connectors to {OUT_DIR}")
        return 0
    failed = False
    for path, text in outputs.items():
        current = path.read_text() if path.exists() else ""
        if current != text:
            failed = True
            diff = difflib.unified_diff(
                current.splitlines(), text.splitlines(),
                fromfile=str(path), tofile=str(path) + " (generated)",
                lineterm="",
            )
            print("\n".join(diff))
    if failed:
        print("connector inventory is out of date: run tools/connector_inventory.py --write")
        return 1
    print(f"connector inventory is current: {len(records)} connectors")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
