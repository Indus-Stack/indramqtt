#!/usr/bin/env bash
# Report the install outcome for the Rust paho-mqtt client.
#
# Use: bash run.sh [transport]
#
# The paho-mqtt Rust crate links the Eclipse Paho C library. Building
# that library needs a C toolchain install and system header changes.
# This container forbids root changes to the system, so the crate is
# not installable here. Each check reports not-supported with that
# reason. The task records the outcome; it does not vendor the crate.
set -u

echo "rust backend: paho-mqtt not installable (needs the Paho C library and system changes)"
for check in v311-connect v5-connect qos0 qos1 qos2 retained will \
    session-resume shared payload-1mib auth keepalive restart; do
  echo "RESULT $check not-supported paho-mqtt rust crate needs the Paho C library; system changes are forbidden"
done
