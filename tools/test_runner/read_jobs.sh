#!/usr/bin/env bash
# This script reads jobs.config

set -e

# Go to the repo directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../..

erlc tools/test_runner/read_jobs.erl
erl -noinput \
    -pa tools/test_runner \
    -s read_jobs main $@
