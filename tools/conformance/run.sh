#!/usr/bin/env bash
# Run the public MQTT interoperability suite against the broker.
#
# The script starts the broker from a generated indra.toml and runs the
# MQTT 3.1.1 file and the MQTT 5.0 file of the Eclipse Paho
# interoperability suite against it. It writes one row per test to
# tools/conformance/results.md with the result and, for a failure, the
# assertion text and the MQTT specification section.
#
# Each test method also runs in its own process with its own timeout.
# The MQTT 5.0 file hangs in `test_flow_control2` against this broker;
# a whole-file run alone would stop there and hide every later test.
# The per-test pass keeps one hang from hiding the other 26 tests. The
# whole-file run is also attempted and its outcome is reported.
#
# The script does not build the broker. The pipeline builds it. The
# script finds the built binary. This keeps one build daemon and stops a
# second target directory inside the tree.
#
# Usage:
#   tools/conformance/run.sh
#
# Environment:
#   INDRA_BROKER_BIN     broker binary (default: the built one below)
#   PAHO_DIR             suite directory (default /work/ext/paho.mqtt.testing)
#   CONFORMANCE_WHOLE_TIMEOUT  seconds for a whole-file run (default 180)
#   CONFORMANCE_TEST_TIMEOUT   seconds for one isolated test (default 120)
#   CONFORMANCE_KEEP     keep the broker log and the raw suite logs
set -u

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
TREE_DIR=$(cd "$SCRIPT_DIR/../.." && pwd)
PAHO_DIR=${PAHO_DIR:-/work/ext/paho.mqtt.testing}
SUITE="$PAHO_DIR/interoperability"
OUT="$TREE_DIR/tools/conformance/results.md"
WHOLE_TIMEOUT=${CONFORMANCE_WHOLE_TIMEOUT:-180}
TEST_TIMEOUT=${CONFORMANCE_TEST_TIMEOUT:-120}

# The suite connects to localhost:1883. `client_test.py` passes an
# unknown `-h` through to unittest, so the host and the port cannot go
# through the command line. The broker binds the default MQTT port.
MQTT_PORT=1883

find_binary() {
    if [ -n "${INDRA_BROKER_BIN:-}" ]; then
        printf '%s' "$INDRA_BROKER_BIN"
        return
    fi
    local target=${CARGO_TARGET_DIR:-}
    if [ -n "$target" ]; then
        for profile in debug release; do
            if [ -x "$target/$profile/indramqtt" ]; then
                printf '%s' "$target/$profile/indramqtt"
                return
            fi
        done
    fi
    printf ''
}

BIN=$(find_binary)
if [ -z "$BIN" ] || [ ! -x "$BIN" ]; then
    echo "FAIL: no broker binary found. Set INDRA_BROKER_BIN or build with the pipeline first."
    exit 2
fi

command -v python3 >/dev/null || { echo "FAIL: python3 is not in PATH"; exit 2; }
[ -d "$SUITE" ] || { echo "FAIL: suite not found at $SUITE. Run tools/conformance/fetch.sh first."; exit 2; }
for f in client_test.py client_test5.py; do
    [ -f "$SUITE/$f" ] || { echo "FAIL: $SUITE/$f is missing"; exit 2; }
done

ROOT=$(mktemp -d)
BROKER_PID=
cleanup() {
    [ -n "$BROKER_PID" ] && kill "$BROKER_PID" 2>/dev/null
    wait 2>/dev/null
    rm -rf "$ROOT"
}
trap cleanup EXIT

read -r API_PORT <<EOF
$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)
EOF

API_KEY=conformance-key-0001
mkdir -p "$ROOT/cfg" "$ROOT/data"
cat > "$ROOT/cfg/indra.toml" <<EOF
[node]
id = "conformance"
data_dir = "$ROOT/data"

[listeners.tcp]
bind = "127.0.0.1:$MQTT_PORT"

[listeners.api]
bind = "127.0.0.1:$API_PORT"

[logging]
level = "warn"

# The suite connects with no credentials. The broker is empty, so
# anonymous access is the only way in.
[auth]
allow_anonymous = true
EOF

echo "run: broker binary $BIN"
echo "run: suite $SUITE"
env -u INDRA_BROKER_BIN INDRA_API_KEYS=$API_KEY INDRA_API_KEY=$API_KEY \
    "$BIN" --config-dir "$ROOT/cfg" --allow-anonymous > "$ROOT/broker.log" 2>&1 &
BROKER_PID=$!

ready=0
for _ in $(seq 1 100); do
    health=$(python3 - "$API_PORT" <<'PY' 2>/dev/null
import sys, urllib.request
print(urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/healthz", timeout=1).read().decode())
PY
)
    if [ "$health" = OK ]; then
        ready=1
        break
    fi
    kill -0 "$BROKER_PID" 2>/dev/null || break
    sleep 0.2
done

if [ "$ready" != 1 ]; then
    echo "FAIL: the broker did not become ready"
    tail -20 "$ROOT/broker.log" 2>/dev/null
    exit 1
fi

# The suite's subscribe-failure tests need one denied filter. The
# reference test broker denies `test/nosubscribe` in its own code. Our
# broker denies through the ordered ACL: the deny entry comes first, the
# allow-all entry second, so the stored no-match default stays deny.
api() {
    method=$1
    path=$2
    body=$3
    python3 - "$API_PORT" "$API_KEY" "$method" "$path" "$body" <<'PY'
import sys, urllib.request
port, key, method, path, body = sys.argv[1:6]
req = urllib.request.Request(
    f"http://127.0.0.1:{port}{path}", method=method,
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    data=body.encode() if body else None)
try:
    with urllib.request.urlopen(req) as res:
        print(res.status)
except urllib.error.HTTPError as err:
    print(err.code)
PY
}

status=$(api POST /api/v1/auth/acls '{"client_pattern":"*","action":"subscribe","topic_pattern":"test/nosubscribe","allow":false}')
if [ "$status" != 201 ]; then
    echo "FAIL: cannot create the deny rule for test/nosubscribe (status $status)"
    exit 1
fi
status=$(api POST /api/v1/auth/acls '{"client_pattern":"*","action":"all","topic_pattern":"#","allow":true}')
if [ "$status" != 201 ]; then
    echo "FAIL: cannot create the allow-all rule (status $status)"
    exit 1
fi

COMMIT=$(cd "$PAHO_DIR" && git rev-parse HEAD 2>/dev/null || echo unknown)
STAMP=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

echo "run: MQTT 3.1.1 file and MQTT 5.0 file"
python3 - "$SUITE" "$ROOT" "$OUT" "$BIN" "$COMMIT" "$STAMP" "$WHOLE_TIMEOUT" "$TEST_TIMEOUT" <<'PY'
import ast
import os
import re
import subprocess
import sys

suite, root, out, binary, commit, stamp, whole_to, test_to = sys.argv[1:9]
whole_timeout = int(whole_to)
test_timeout = int(test_to)

# file -> run unittest verbose? client_test.py rejects -v (its getopt
# string has no v), client_test5.py accepts and forwards it.
FILES = [("client_test.py", False), ("client_test5.py", True)]

# The MQTT specification section each test concerns. The reference is
# the section of the MQTT specification the test exercises, whether the
# test passes or fails.
SPEC = {
    "client_test.py": {
        "testBasic": "MQTT 3.1.1 \u00a73.1\u20133.2, \u00a73.8 (CONNECT, SUBSCRIBE, PUBLISH)",
        "test_retained_messages": "MQTT 3.1.1 \u00a73.3.1.3 (RETAIN)",
        "test_zero_length_clientid": "MQTT 3.1.1 \u00a73.1.3.1 [MQTT-3.1.3-6] (zero-length ClientId)",
        "test_offline_message_queueing": "MQTT 3.1.1 \u00a73.1.2.4, \u00a74.1 (persistent session queueing)",
        "test_overlapping_subscriptions": "MQTT 3.1.1 \u00a73.8.4 (overlapping subscriptions)",
        "test_keepalive": "MQTT 3.1.1 \u00a73.1.2.10 (Keep Alive)",
        "test_redelivery_on_reconnect": "MQTT 3.1.1 \u00a74.4 (message delivery retry)",
        "test_subscribe_failure": "MQTT 3.1.1 \u00a73.9.3 [MQTT-3.9.3-2] (SUBACK return code 0x80)",
        "test_dollar_topics": "MQTT 3.1.1 \u00a74.7.2 [MQTT-4.7.2-1] (wildcard and $ topics)",
        "test_unsubscribe": "MQTT 3.1.1 \u00a73.10 (UNSUBSCRIBE, UNSUBACK)",
    },
    "client_test5.py": {
        "test_basic": "MQTT 5.0 \u00a73.1\u20133.2, \u00a73.8",
        "test_retained_message": "MQTT 5.0 \u00a73.3.1.3 (RETAIN)",
        "test_will_message": "MQTT 5.0 \u00a73.1.2.5 (Will Message)",
        "test_zero_length_clientid": "MQTT 5.0 \u00a73.1.3.1 (Client Identifier)",
        "test_offline_message_queueing": "MQTT 5.0 \u00a74.1 (session state queueing)",
        "test_overlapping_subscriptions": "MQTT 5.0 \u00a73.8.4 (overlapping subscriptions)",
        "test_keepalive": "MQTT 5.0 \u00a73.1.2.10 (Keep Alive)",
        "test_redelivery_on_reconnect": "MQTT 5.0 \u00a74.4 (message delivery retry)",
        "test_subscribe_failure": "MQTT 5.0 \u00a73.9.3 (SUBACK reason code)",
        "test_dollar_topics": "MQTT 5.0 \u00a74.7.2 (wildcard and $ topics)",
        "test_unsubscribe": "MQTT 5.0 \u00a73.10 (UNSUBSCRIBE, UNSUBACK)",
        "test_session_expiry": "MQTT 5.0 \u00a73.1.2.11.2 (Session Expiry Interval)",
        "test_user_properties": "MQTT 5.0 \u00a73.3.2.3.7 (User Property)",
        "test_payload_format": "MQTT 5.0 \u00a73.3.2.3.2 (Payload Format Indicator)",
        "test_publication_expiry": "MQTT 5.0 \u00a73.3.2.3.3 (Message Expiry Interval)",
        "test_subscribe_options": "MQTT 5.0 \u00a73.8.3.1 (Subscription Options)",
        "test_assigned_clientid": "MQTT 5.0 \u00a73.1.3.1.3 (Assigned Client Identifier)",
        "test_subscribe_identifiers": "MQTT 5.0 \u00a73.3.2.3.8 (Subscription Identifier)",
        "test_request_response": "MQTT 5.0 \u00a73.3.2.3.5\u20133.6 (Response Topic, Correlation Data)",
        "test_client_topic_alias": "MQTT 5.0 \u00a73.3.2.3.4 (Topic Alias)",
        "test_server_topic_alias": "MQTT 5.0 \u00a73.3.2.3.4 (Topic Alias)",
        "test_maximum_packet_size": "MQTT 5.0 \u00a73.1.2.11.4 (Maximum Packet Size)",
        "test_server_keep_alive": "MQTT 5.0 \u00a73.2.2.3.14 (Server Keep Alive)",
        "test_flow_control1": "MQTT 5.0 \u00a74.9 (Receive Maximum, flow control)",
        "test_flow_control2": "MQTT 5.0 \u00a74.9 [MQTT-4.9.0-1] (Receive Maximum exceeded)",
        "test_will_delay": "MQTT 5.0 \u00a73.1.3.2.2 (Will Delay Interval)",
        "test_shared_subscriptions": "MQTT 5.0 \u00a74.8.2 (Shared Subscriptions)",
    },
}

# For each test that can fail, who is right and the smallest correction.
# The report prints an entry only for a test that failed in this run.
ANALYSIS = {
    "client_test.py": {
        "test_dollar_topics": "The broker is wrong. [MQTT-4.7.2-1] forbids a "
            "match between a filter that starts with a wildcard and a topic "
            "that starts with `$`. The broker delivered `$TopicA/B` to the "
            "filter `+/+`. Correct the router match. Class: large.",
        "test_offline_message_queueing": "The broker is wrong. MQTT 3.1.1 "
            "\u00a73.1.2.4 and \u00a74.1 require a persistent session to keep "
            "its subscriptions and to queue QoS 1 and QoS 2 messages. The "
            "broker queued nothing. Class: large.",
        "test_redelivery_on_reconnect": "The broker is wrong. MQTT 3.1.1 "
            "\u00a74.4 requires a resend of unacknowledged QoS 1 and QoS 2 "
            "messages on reconnect. The broker resent nothing. Class: large.",
        "test_subscribe_failure": "The broker was wrong. [MQTT-3.9.3-2] allows "
            "only 0, 1, 2 and 0x80 in a version 3 SUBACK. The broker sent "
            "0x87. Corrected in this task: a version 3 denial answers 0x80, "
            "with a socket regression test. This recorded row predates the fix.",
        "test_zero_length_clientid": "The broker is wrong. [MQTT-3.1.3-6] and "
            "[MQTT-3.1.3-7] require a reject with CONNACK code 0x02 for a "
            "zero-length ClientId with CleanSession 0. The broker accepted "
            "it. Class: medium.",
    },
    "client_test5.py": {
        "test_assigned_clientid": "The broker is wrong. MQTT 5.0 \u00a73.1.3.1.3 "
            "requires an Assigned Client Identifier in CONNACK when the server "
            "accepts an empty ClientId. The broker sent none. Class: medium.",
        "test_client_topic_alias": "The broker is wrong. MQTT 5.0 \u00a73.3.2.3.4 "
            "makes a Topic Alias of 0 and an unmapped alias a protocol error. "
            "The broker ignored the alias and did not disconnect. Class: medium.",
        "test_dollar_topics": "The broker is wrong. MQTT 5.0 \u00a74.7.2 forbids "
            "a match between a wildcard filter and a `$` topic. The broker "
            "delivered `$TopicA/B` to the filter `+/+`. Class: large.",
        "test_flow_control1": "The broker is wrong. MQTT 5.0 \u00a74.9 requires "
            "the release of a held message after the client acknowledges one. "
            "The broker never released the held publish. Class: medium.",
        "test_flow_control2": "The broker is wrong. MQTT 5.0 \u00a74.9 requires a "
            "DISCONNECT with reason 0x93 when the client exceeds the Receive "
            "Maximum. The broker kept the connection. Class: medium.",
        "test_payload_format": "The broker is wrong. MQTT 5.0 \u00a73.3.2.3.2 and "
            "\u00a73.3.2.3.9 require the Payload Format Indicator and the "
            "Content Type on delivery. The broker forwarded neither. Class: medium.",
        "test_publication_expiry": "The broker is wrong. MQTT 5.0 \u00a73.3.2.3.3 "
            "requires the drop of a queued message whose Message Expiry "
            "Interval passed. The broker delivered all four. Class: medium.",
        "test_request_response": "The broker is wrong. MQTT 5.0 \u00a73.3.2.3.5 and "
            "\u00a73.3.2.3.6 require the Response Topic and the Correlation "
            "Data on delivery. The broker forwarded neither. Class: medium.",
        "test_retained_message": "The broker is wrong. MQTT 5.0 \u00a73.3.2.3.7 "
            "requires the User Properties on a retained message. The broker "
            "lost them. Class: medium.",
        "test_server_keep_alive": "The broker is right. MQTT 5.0 \u00a73.2.2.3.14 "
            "makes the Server Keep Alive an optional CONNACK property. The "
            "broker accepted the client's Keep Alive of 120 and sent no "
            "property, which the specification permits. The suite expects its "
            "own test broker to announce 60. No correction.",
        "test_server_topic_alias": "The broker is wrong. MQTT 5.0 \u00a73.3.2.3.4 "
            "lets the server use outbound topic aliases up to the client "
            "maximum. The broker sent the full topic each time. Class: medium.",
        "test_session_expiry": "The broker is wrong. MQTT 5.0 \u00a73.1.2.11.2 "
            "requires the Session Expiry Interval and its DISCONNECT override. "
            "The session-present flag was wrong. Class: medium.",
        "test_subscribe_failure": "The broker differs. The suite expects 0x80; "
            "the broker sent 0x87. Both are legal in version 5, but the "
            "interoperability target uses 0x80. Send 0x80. Class: small.",
        "test_subscribe_identifiers": "The broker differs. MQTT 5.0 \u00a73.8.4 "
            "permits one copy per matching subscription; the suite expects one "
            "copy with every Subscription Identifier. The broker sent two "
            "copies. Coalesce the overlap. Class: medium.",
        "test_user_properties": "The broker is wrong. MQTT 5.0 \u00a73.3.2.3.7 "
            "requires the User Properties on delivery. The broker forwarded "
            "none. Class: medium.",
        "test_will_delay": "The broker is wrong. MQTT 5.0 \u00a73.1.3.2.2 requires "
            "a delay of the will by the Will Delay Interval. The broker sent "
            "the will at once. Class: medium.",
        "test_will_message": "The broker is wrong. MQTT 5.0 \u00a73.1.2.5 requires "
            "the User Properties of the will. The broker sent none. Class: medium.",
    },
}


def test_names(fname):
    tree = ast.parse(open(os.path.join(suite, fname), encoding="utf-8").read())
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "Test":
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name.startswith("test"):
                    names.append(item.name)
    names.sort()
    return names


def extract_reasons(text):
    """Map a failed test name to its assertion or exception line."""
    reasons = {}
    lines = text.splitlines()
    starts = []
    for i, ln in enumerate(lines):
        m = re.match(r"^(FAIL|ERROR): (\w+)", ln)
        if m:
            starts.append((i, m.group(2)))
    for idx, (i, name) in enumerate(starts):
        end = starts[idx + 1][0] if idx + 1 < len(starts) else len(lines)
        chunk = lines[i + 1:end]
        # The header can be followed by the docstring before the dashes.
        sep = next((j for j, ln in enumerate(chunk)
                    if set(ln.strip()) == {"-"}), None)
        body = chunk[sep + 1:] if sep is not None else chunk
        body = [ln.strip() for ln in body
                if ln.strip() and set(ln.strip()) - set("-=")]
        # Prefer the exception line over a diff line or the summary.
        exc = [ln for ln in body
               if re.match(r"^[A-Za-z_][\w.]*(Error|Exception)\b", ln)]
        reasons[name] = exc[-1] if exc else (body[-1] if body else name)
    return reasons


def analyze(text, names, complete_ok):
    """Return {name: (status, reason) or None}. None means not run."""
    reasons = extract_reasons(text)
    failblock = {}
    for kind, name in re.findall(r"^(FAIL|ERROR): (\w+)", text, re.M):
        failblock.setdefault(name, kind)
    verbose = {}
    for m in re.finditer(
        r"^(test_\w+) \(.*?\) \.\.\. (ok|FAIL|ERROR|skipped[^\n]*)", text, re.M):
        verbose[m.group(1)] = m.group(2)
    # The run is complete only when unittest reports as many tests as
    # this file has. A "Ran 0 tests" line is not a complete run.
    ran = re.search(r"^Ran (\d+) tests? in", text, re.M)
    complete = (complete_ok and ran is not None
                and int(ran.group(1)) == len(names))

    result = {}
    for n in names:
        if n in failblock:
            result[n] = ("fail" if failblock[n] == "FAIL" else "error",
                         reasons.get(n, failblock[n]))
        elif n in verbose:
            v = verbose[n]
            status = {"ok": "pass", "FAIL": "fail", "ERROR": "error"}.get(v, "skipped")
            result[n] = (status, reasons.get(n, ""))
        elif complete:
            result[n] = ("pass", "")
        else:
            result[n] = None
    return result, complete


def run_capture(args, timeout):
    """Run args in the suite dir. Return (text, timed_out)."""
    try:
        p = subprocess.run(args, cwd=suite, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, timeout=timeout)
        return p.stdout.decode("utf-8", "replace"), False
    except subprocess.TimeoutExpired as err:
        data = err.stdout or b""
        return data.decode("utf-8", "replace"), True


report = {}
whole_note = {}
for fname, verbose in FILES:
    names = test_names(fname)
    base_args = [sys.executable, "-u", fname]
    if verbose:
        base_args.append("-v")

    whole_text, whole_timed_out = run_capture(base_args, whole_timeout)
    with open(os.path.join(root, fname + ".whole.log"), "w", encoding="utf-8") as f:
        f.write(whole_text)
    whole_result, whole_complete = analyze(whole_text, names, not whole_timed_out)

    results = {}
    source = {}
    if whole_complete:
        # The file ran end to end: report its own per-test verdicts.
        for n in names:
            results[n] = whole_result.get(n) or ("pass", "")
            source[n] = "suite run"
    else:
        # The whole-file run stopped before the end (a hang or a crash).
        # Run every test alone so no test is hidden and each failure
        # carries its own assertion text.
        for n in names:
            iso_args = [sys.executable, "-u", fname, "Test." + n]
            if verbose:
                iso_args.append("-v")
            iso_text, iso_timed_out = run_capture(iso_args, test_timeout)
            with open(os.path.join(root, fname + "." + n + ".iso.log"), "w",
                      encoding="utf-8") as f:
                f.write(iso_text)
            iso_result, _ = analyze(iso_text, [n], not iso_timed_out)
            r = iso_result.get(n)
            if r is None:
                r = ("fail", "test did not finish within %ds (hang)" % test_timeout)
            results[n] = r
            source[n] = "isolated"

    # Summary of the whole-file attempt.
    if whole_complete:
        wpass = sum(1 for n in names if whole_result.get(n, ("",))[0] == "pass")
        wfail = sum(1 for n in names
                    if whole_result.get(n, ("",))[0] in ("fail", "error"))
        whole_note[fname] = ("completed: %d pass, %d fail of %d tests"
                             % (wpass, wfail, len(names)))
    else:
        # The last test the whole run started is the one it stopped on.
        last = None
        for m in re.finditer(r"^(test_\w+) \(.*?\) \.\.\.",
                             whole_text, re.M):
            last = m.group(1)
        whole_note[fname] = ("stopped early (timed out after %ds)%s"
                             % (whole_timeout,
                                "; last test started: `%s`" % last if last else ""))
    report[fname] = (names, results, source)


def tally(results):
    p = sum(1 for r in results.values() if r[0] == "pass")
    f = sum(1 for r in results.values() if r[0] in ("fail", "error"))
    return p, f

with open(out, "w", encoding="utf-8") as f:
    f.write("# Public MQTT conformance run\n\n")
    f.write("A real run of the Eclipse Paho interoperability suite against the\n")
    f.write("built broker. `tools/conformance/run.sh` writes this file.\n\n")
    f.write("- Date (UTC): %s\n" % stamp)
    f.write("- Broker binary: `%s`\n" % binary)
    f.write("- Suite: `%s`\n" % suite)
    f.write("- Suite commit: `%s`\n" % commit)
    f.write("- Broker settings: `listeners.tcp` on 127.0.0.1:1883, "
            "`auth.allow_anonymous = true`, one deny ACL entry for "
            "`test/nosubscribe` and one allow-all ACL entry.\n")
    f.write("- Method: each test runs in its own process with a %ds timeout, "
            "so one hang cannot hide the other tests. The whole-file run is "
            "also attempted with a %ds timeout and reported below.\n\n"
            % (test_timeout, whole_timeout))
    f.write("## Whole-file runs\n\n")
    for fname, _ in FILES:
        f.write("- `%s`: %s\n" % (fname, whole_note[fname]))
    f.write("\n## Results\n\n")
    f.write("| File | Test | Result | How run | Assertion on failure | MQTT specification section |\n")
    f.write("|---|---|---|---|---|---|\n")
    for fname, _ in FILES:
        names, results, source = report[fname]
        for n in names:
            status, reason = results[n]
            reason_cell = reason.replace("|", "\\|") if reason else ""
            spec = SPEC.get(fname, {}).get(n, "see MQTT specification")
            f.write("| `%s` | `%s` | %s | %s | %s | %s |\n"
                    % (fname, n, status, source[n], reason_cell, spec))
    f.write("\n## Totals\n\n")
    tp = tf = 0
    for fname, _ in FILES:
        names, results, _ = report[fname]
        p, fl = tally(results)
        tp += p
        tf += fl
        f.write("- `%s`: %d pass, %d fail of %d tests.\n"
                % (fname, p, fl, len(names)))
    f.write("- Both files: %d pass, %d fail of %d tests.\n"
            % (tp, tf, tp + tf))
    f.write("\n## Failure analysis\n\n")
    any_failure = False
    for fname, _ in FILES:
        names, results, _ = report[fname]
        for n in names:
            status, reason = results[n]
            if status not in ("fail", "error"):
                continue
            any_failure = True
            text = ANALYSIS.get(fname, {}).get(n)
            f.write("- `%s` / `%s`: %s\n" % (fname, n, reason or status))
            if text:
                f.write("  - %s\n" % text)
            else:
                f.write("  - No analysis recorded for this test.\n")
    if not any_failure:
        f.write("No test failed.\n")

print("results.md: %d pass, %d fail of %d tests" % (tp, tf, tp + tf))
for fname, _ in FILES:
    print("  %s: %s" % (fname, whole_note[fname]))
PY
DRIVER_RC=$?

if [ -n "${CONFORMANCE_KEEP:-}" ]; then
    mkdir -p "$TREE_DIR/tools/conformance/logs"
    cp "$ROOT"/*.log "$TREE_DIR/tools/conformance/logs/" 2>/dev/null
fi

echo "run: results written to $OUT"
exit "$DRIVER_RC"
