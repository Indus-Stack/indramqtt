#!/usr/bin/env bash
set -euo pipefail
OUT_DIR="${1:?usage: gen_test_pki.sh <out_dir>}"
mkdir -p "$OUT_DIR"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=indramqtt-test-ca" \
    -keyout "$OUT_DIR/ca-key.pem" -out "$OUT_DIR/ca-cert.pem" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
    -keyout "$OUT_DIR/server-key.pem" -out "$OUT_DIR/server.csr" 2>/dev/null
printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n" > "$OUT_DIR/ext.cnf"
openssl x509 -req -in "$OUT_DIR/server.csr" -CA "$OUT_DIR/ca-cert.pem" -CAkey "$OUT_DIR/ca-key.pem" \
    -CAcreateserial -days 1 -extfile "$OUT_DIR/ext.cnf" -out "$OUT_DIR/server-cert.pem" 2>/dev/null
rm -f "$OUT_DIR/server.csr" "$OUT_DIR/ext.cnf" "$OUT_DIR/ca-cert.srl"
