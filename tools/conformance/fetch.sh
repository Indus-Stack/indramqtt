#!/usr/bin/env bash
# Fetch the public interoperability suite at a pinned commit.
#
# The suite is an outside project. The script downloads it into a
# directory outside this tree. It never copies the suite into the
# tree. The script verifies the commit and the SHA-256 of the two
# test files that run.sh uses. It then prints the licence file name.
#
# Usage:
#   tools/conformance/fetch.sh
#
# Environment:
#   PAHO_DIR   target directory (default /work/ext/paho.mqtt.testing)
#
# The script exits 2 for bad usage or a missing tool. It exits 1 when
# the download, the commit check or a SHA-256 check fails.
set -u

REPO_URL=https://github.com/eclipse/paho.mqtt.testing.git
# Pinned commit: the upstream master head at the time of writing.
PINNED_COMMIT=9d7bb80bb8b9d9cfc0b52f8cb4c1916401281103

# SHA-256 of the two test files that run.sh runs. The hash pins the
# exact suite content, so a changed upstream master cannot change a
# result. It also checks the download.
CLIENT_TEST_SHA=601ff53754d52d2ac9750f75562bade184022495a2139b80553795ae1bb6184d
CLIENT_TEST5_SHA=cf221695739610641615dfefe13981a537addf52cd693a212f9181e70e0395bc

PAHO_DIR=${PAHO_DIR:-/work/ext/paho.mqtt.testing}
PARENT=$(dirname "$PAHO_DIR")

command -v git >/dev/null || { echo "FAIL: git is not in PATH"; exit 2; }
command -v sha256sum >/dev/null || { echo "FAIL: sha256sum is not in PATH"; exit 2; }

mkdir -p "$PARENT" || { echo "FAIL: cannot create $PARENT"; exit 2; }

if [ ! -d "$PAHO_DIR/.git" ]; then
    echo "fetch: cloning $REPO_URL into $PAHO_DIR"
    git clone --quiet "$REPO_URL" "$PAHO_DIR" || {
        echo "FAIL: clone of $REPO_URL failed"
        exit 1
    }
fi

# Make sure the pinned commit is present. A full clone has it when the
# branch moved on; a fetch of the commit covers the other case.
if ! git -C "$PAHO_DIR" cat-file -e "${PINNED_COMMIT}^{commit}" 2>/dev/null; then
    echo "fetch: getting pinned commit $PINNED_COMMIT"
    git -C "$PAHO_DIR" fetch --quiet origin "$PINNED_COMMIT" || {
        echo "FAIL: cannot fetch commit $PINNED_COMMIT"
        exit 1
    }
fi

# Materialize the pinned tree without changing branch state. `git
# archive` writes the exact commit content into the directory. The
# `.git` directory stays as it is.
echo "fetch: checking out pinned tree into the working files"
git -C "$PAHO_DIR" archive "$PINNED_COMMIT" | tar -x -C "$PAHO_DIR" || {
    echo "FAIL: cannot extract commit $PINNED_COMMIT"
    exit 1
}

# The commit check. The fetched object must resolve to the pinned commit.
RESOLVED=$(git -C "$PAHO_DIR" rev-parse "${PINNED_COMMIT}^{commit}" 2>/dev/null)
if [ "$RESOLVED" != "$PINNED_COMMIT" ]; then
    echo "FAIL: $PAHO_DIR does not hold commit $PINNED_COMMIT"
    exit 1
fi

check_sha() {
    local file=$1 want=$2
    local path="$PAHO_DIR/$file"
    if [ ! -f "$path" ]; then
        echo "FAIL: $path is missing"
        return 1
    fi
    local got
    got=$(sha256sum "$path" | awk '{print $1}')
    if [ "$got" != "$want" ]; then
        echo "FAIL: SHA-256 of $file is $got, expected $want"
        return 1
    fi
    echo "ok   $file SHA-256 $got"
    return 0
}

failures=0
check_sha interoperability/client_test.py "$CLIENT_TEST_SHA" || failures=$((failures + 1))
check_sha interoperability/client_test5.py "$CLIENT_TEST5_SHA" || failures=$((failures + 1))

# The licence file name. The suite is dual-licensed; the file is LICENSE.
LICENCE=LICENSE
if [ ! -f "$PAHO_DIR/$LICENCE" ]; then
    echo "FAIL: licence file $PAHO_DIR/$LICENCE is missing"
    failures=$((failures + 1))
fi

if [ "$failures" != 0 ]; then
    echo "fetch: $failures check(s) failed"
    exit 1
fi

echo "suite: $PAHO_DIR"
echo "commit: $PINNED_COMMIT"
echo "licence file: $LICENCE"
exit 0
