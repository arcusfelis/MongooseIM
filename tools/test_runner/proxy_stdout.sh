#!/usr/bin/env bash

# Helps with "docker exec" issue
# https://github.com/drud/ddev/issues/732

TMP_DIR="$(mktemp -d)"
touch "$TMP_DIR/stdout" "$TMP_DIR/stderr"

tail -q -f "$TMP_DIR/stdout" "$TMP_DIR/stderr" &
TAIL_PID=$!

"$@" >"$TMP_DIR/stdout" 2>"$TMP_DIR/stderr"
EXIT_CODE=$?

rm -f "$TMP_DIR/stdout" "$TMP_DIR/stderr"
rmdir "$TMP_DIR"

kill "$TAIL_PID"

exit $EXIT_CODE
