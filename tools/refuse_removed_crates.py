#!/usr/bin/env python3
"""Refuse removed runtime crates after a full, locked graph check.

Exit 0: absent; exit 1: a refused crate is present; exit 2: no valid proof.
"""

import re
import subprocess
import sys

REFUSED = {("rustls", "0.21.12"), ("rustls-webpki", "0.101.7"), ("sqlx", "0.7.4")}
PACKAGE = re.compile(
    r"([A-Za-z0-9_-]+) v(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?"
    r"(?:\+[0-9A-Za-z.-]+)?)(?:\s.*)?"
)


def main():
    command = [
        "cargo", "+stable", "tree", "--workspace", "--all-features", "--locked",
        "--target", "all", "--edges", "normal", "--prefix", "none", "--format", "{p}",
        "--color", "never",
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as error:
        print(f"BLOCKED: cannot run cargo tree: {error}", file=sys.stderr)
        return 2
    if result.stderr:
        sys.stderr.write(result.stderr)
    if result.returncode:
        print(f"BLOCKED: cargo tree failed with exit {result.returncode}", file=sys.stderr)
        return 2
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        print("BLOCKED: cargo tree returned no packages", file=sys.stderr)
        return 2
    found = set()
    for line in lines:
        package = PACKAGE.fullmatch(line)
        if package is None:
            print(f"BLOCKED: cannot read cargo tree package: {line}", file=sys.stderr)
            return 2
        found.add(package.groups())
    present = sorted(REFUSED & found)
    for name, version in present:
        print(f"ERROR: {name}@{version} is in the all-feature, all-target runtime graph")
    if present:
        return 1
    print("Removed crates are absent from the resolved all-feature, all-target runtime graph")
    return 0


if __name__ == "__main__":
    sys.exit(main())
