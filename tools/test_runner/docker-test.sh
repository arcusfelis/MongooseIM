#!/usr/bin/env bash

# Start nodes and test preset in a separate container
# Takes:
# - ERLANG_VERSION
# - BUILD_VOLUME
# - TEST_CONTAINER_NAM
#
# Spawns a test container.
# Takes build from the volume and test it.
# Assumes that docker-build.sh has been executed before.
# Assumes that database services were registered in the network before.

set -eu

ERLANG_VERSION=${ERLANG_VERSION:-20}
BUILD_VOLUME=${BUILD_VOLUME:-mongooseim-test-build-volume}
TEST_CONTAINER_NAME=${TEST_CONTAINER_NAME:-mongooseim-test}
DOCKER_NETWORK=${DOCKER_NETWORK:-mongoose-network}
RESET_DOCKER_CONTAINERS=${RESET_DOCKER_CONTAINERS:-false}

if [ "$RESET_DOCKER_CONTAINERS" = true ]; then
    docker rm -f "$TEST_CONTAINER_NAME" || true
fi

VARS_FILE=_build/.test_runner/$TEST_CONTAINER_NAME-vars
mkdir -p $(dirname "$VARS_FILE")
export > "$VARS_FILE"

echo "Starting $TEST_CONTAINER_NAME"
docker run -d  \
    -v $(pwd)/$VARS_FILE:/env_vars:ro \
    -v $(pwd)/tools/db_configs/odbc.ini:/root/.odbc.ini:ro \
    --network $DOCKER_NETWORK \
    -v $(pwd):/opt/mongooseim_src \
    -v $BUILD_VOLUME:/opt/mongooseim_build:ro \
    -v ~/.cache/rebar3:/root/.cache/rebar3 \
    --name=$TEST_CONTAINER_NAME \
    erlang:$ERLANG_VERSION \
    tail -F /var/log/progress || echo "Skip starting Test container"

docker exec -i $TEST_CONTAINER_NAME /opt/mongooseim_src/tools/test_runner/docker-test-init.sh

echo "FINISHED $TEST_CONTAINER_NAME"
