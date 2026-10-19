#!/usr/bin/env python3
"""Check the CI entry with controlled Cargo results."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
STEP = "      - name: Refuse the removed vulnerable and unmaintained crates"
REFUSED = ("rustls@0.21.12", "rustls-webpki@0.101.7", "sqlx@0.7.4")
FAKE_CARGO = r"""
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with Path(os.environ['GATE_CALLS']).open('a') as log:
    log.write(json.dumps(args) + '\n')
mode = os.environ['GATE_MODE']
if mode == 'query-error':
    print('package ID specification could not be read: broken manifest', file=sys.stderr)
    sys.exit(101)
if mode == 'empty':
    sys.exit(0)
if mode == 'malformed':
    print('not a package graph')
    sys.exit(0)
present = os.environ.get('GATE_PRESENT', '')
if mode == 'feature-only' and '--all-features' not in args:
    present = ''
if mode == 'target-only' and ('--target' not in args or args[args.index('--target') + 1] != 'all'):
    present = ''
if '-i' in args:
    package = args[args.index('-i') + 1]
    if package == present:
        print(package.replace('@', ' v'))
        sys.exit(0)
    print('package ID specification did not match any packages', file=sys.stderr)
    sys.exit(101)
print('broker-node v0.1.0 (/workspace)')
print('serde v1.0.228')
if present:
    print(present.replace('@', ' v') + ' (*)')
"""


def ci_command():
    lines = (ROOT / '.github/workflows/ci.yml').read_text().splitlines()
    start = lines.index(STEP) + 1
    while not lines[start].lstrip().startswith('run:'):
        start += 1
    value = lines[start].strip().removeprefix('run:').strip()
    if value != '|':
        return value
    command = []
    for line in lines[start + 1:]:
        if line and not line.startswith('          '):
            break
        command.append(line[10:] if line else '')
    return '\n'.join(command)


class RefusalGateTests(unittest.TestCase):
    def run_gate(self, mode='absent', present='', missing=False):
        with tempfile.TemporaryDirectory(prefix='refusal-gate-') as temp:
            directory = Path(temp)
            (directory / 'python3').symlink_to(sys.executable)
            if not missing:
                cargo = directory / 'cargo'
                cargo.write_text('#!' + sys.executable + '\n' + FAKE_CARGO)
                cargo.chmod(0o755)
            env = os.environ.copy()
            env.update(PATH=str(directory), GATE_MODE=mode,
                       GATE_PRESENT=present, GATE_CALLS=str(directory / 'calls'))
            result = subprocess.run(['/bin/bash', '-c', ci_command()], cwd=ROOT,
                                    env=env, capture_output=True, text=True)
            log = directory / 'calls'
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return result, calls

    def test_absent_packages_pass_with_full_locked_graph(self):
        result, calls = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(calls)
        self.assertIn('--all-features', calls[0])
        self.assertIn('--locked', calls[0])
        self.assertEqual(calls[0][calls[0].index('--target') + 1], 'all')
        self.assertIn('normal', calls[0])

    def test_each_refused_package_fails(self):
        for package in REFUSED:
            with self.subTest(package=package):
                result, _ = self.run_gate(present=package)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(package, result.stdout + result.stderr)

    def test_optional_feature_dependency_fails(self):
        result, _ = self.run_gate(mode='feature-only', present=REFUSED[0])
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_other_target_runtime_dependency_fails(self):
        result, _ = self.run_gate(mode='target-only', present=REFUSED[1])
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_query_error_is_blocked_not_absence(self):
        result, _ = self.run_gate(mode='query-error')
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)

    def test_missing_cargo_is_blocked(self):
        result, _ = self.run_gate(missing=True)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)

    def test_empty_success_is_not_proof(self):
        result, _ = self.run_gate(mode='empty')
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)

    def test_malformed_success_is_not_proof(self):
        result, _ = self.run_gate(mode='malformed')
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)

    def test_other_versions_are_not_the_refused_versions(self):
        result, _ = self.run_gate(present='rustls@0.23.35')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
