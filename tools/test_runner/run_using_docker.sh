#!/usr/bin/env bash

set -eu
source tools/travis-common-vars.sh
source tools/test_runner/helpers.sh

ERLANG_VERSIONS="$(tools/test_runner/read_jobs.sh list_erlang_versions)"
echo "ERLANG_VERSIONS=$ERLANG_VERSIONS"

# Filters out unknown jobs
FINAL_JOBS="$(tools/test_runner/read_jobs.sh list_jobs)"
echo "FINAL_JOBS=$FINAL_JOBS"

function build_for_erlang_version
{
    export ERLANG_VERSION=$1
    export BUILD_VOLUME=mongooseim-build-volume-erlang$ERLANG_VERSION
    export BUILD_CONTAINER_NAME=mongooseim-builder-erlang$ERLANG_VERSION

    # Takes:
    # - ERLANG_VERSION
    # - BUILD_VOLUME
    # - BUILD_CONTAINER_NAME
    ./tools/test_runner/docker-build.sh
}

function make_job_env_file
{
    export JOB=$1
    local JOB_VARS_FILE=_build/.test_runner/job-${JOB}-vars
    echo "JOB_VARS_FILE=$JOB_VARS_FILE"
    mkdir -p "_build/.test_runner/"
    tools/test_runner/read_jobs.sh read_variables "$JOB" > "$JOB_VARS_FILE"
}

function import_job_env_vars
{
    export JOB=$1
    local JOB_VARS_FILE=_build/.test_runner/job-${JOB}-vars

    # -o allexport enables all following variable definitions to be exported.
    # +o allexport disables this feature.
    set -o allexport
    source "$JOB_VARS_FILE"
    set +o allexport

    export TEST_CONTAINER_NAME=mongooseim-test-job-$JOB
    export BUILD_VOLUME=mongooseim-build-volume-erlang$ERLANG_VERSION
    export DOCKER_NAME_PREFIX=-job-$JOB
    export PUBLISH_PORTS=false
    export DOCKER_NETWORK=mongooseim-job-$JOB
}

# Uses subshell to not contaminate the main shell with env vars
function setup_job_db
{(
    JOB=$1
    import_job_env_vars $JOB
    docker network create $DOCKER_NETWORK || echo "Skipping creating network"
    ./tools/travis-setup-db.sh
)}

# Uses subshell to not contaminate the main shell with env vars
function run_test_job
{(
    set -eu
    JOB=$1
    import_job_env_vars $JOB

    # Takes:
    # - ERLANG_VERSION
    # - BUILD_VOLUME
    # - TEST_CONTAINER_NAME
    buffered_async_helper "job_$JOB" ./tools/test_runner/docker-test.sh
)}

function setup_db
{
    for JOB in $FINAL_JOBS; do
       setup_job_db "$JOB"
    done
}

for JOB in $FINAL_JOBS; do
    make_job_env_file "$JOB"
done

pids=()
for ERLANG_VERSION in $ERLANG_VERSIONS; do
    buffered_async_helper "build_$ERLANG_VERSION" build_for_erlang_version "$ERLANG_VERSION" &
    pids+=("$!")
done

# Do setup_db in parallel with compilation
# Don't start DB-s in parallel though yet
buffered_async_helper "setup_db" setup_db &
pids+=("$!")

./tools/kill_processes_on_exit.sh $ROOT_SCRIPT_PID "${pids[@]}" &
wait_for_pids "${pids[@]}"

pids=()
for JOB in $FINAL_JOBS; do
    echo "START_JOB: $JOB"
    run_test_job "$JOB" &
    pid="$!"
    describe_pid "$pid" " {run_test_job job_$JOB} "
    pids+=($pid)
done
tools/kill_processes_on_exit.sh "$ROOT_SCRIPT_PID" "${pids[@]}" &
wait_for_pids "${pids[@]}"
