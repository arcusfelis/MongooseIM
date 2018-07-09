#!/usr/bin/env bash
# This file creates two spec files:
# - auto_big_tests.spec
# - auto_small_tests.spec

set -e

# Go to the repo directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

echo "Selected tests: $@"

rm -f auto_small_tests.spec big_tests/auto_big_tests.spec

# Fill default specs
echo "" > auto_small_tests.spec
cp big_tests/default.spec big_tests/auto_big_tests.spec

# If there are arguments
if [ "$#" -ne 0 ]; then
erlc tools/test_runner/selected_tests_to_test_spec.erl
erl -noinput \
    -pa tools/test_runner \
    -pa _build/test/lib/mongooseim/test/ \
    -pa big_tests/tests/ \
    -s selected_tests_to_test_spec main $@
fi
