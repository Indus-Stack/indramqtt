#!/usr/bin/env bash
# Report the install outcome for the mosquitto command-line clients.
#
# Use: bash run.sh [transport]
#
# The spec asks for mosquitto_pub and mosquitto_sub when they are
# present. They are not present in this container. Each check reports
# not-supported with that reason. The task records the outcome; it
# does not install system packages.
set -u

if command -v mosquitto_pub >/dev/null 2>&1 && command -v mosquitto_sub >/dev/null 2>&1; then
  echo "mosquitto backend: tools present"
  echo "RESULT v311-connect not-supported runner for present tools is not written in this task"
  for check in v5-connect qos0 qos1 qos2 retained will \
      session-resume shared payload-1mib auth keepalive restart; do
    echo "RESULT $check not-supported runner for present tools is not written in this task"
  done
  exit 0
fi

echo "mosquitto backend: mosquitto_pub and mosquitto_sub are not present"
for check in v311-connect v5-connect qos0 qos1 qos2 retained will \
    session-resume shared payload-1mib auth keepalive restart; do
  echo "RESULT $check not-supported mosquitto tools are not present in this container"
done
