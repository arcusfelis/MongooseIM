#!/usr/bin/env bash

# Takes:
# - ERLANG_VERSION
# - BUILD_VOLUME
# - BUILD_CONTAINER_NAME
#
# Spawns a build container
# Writes build on the docker volume
# Keeps the container running

set -eu

ERLANG_VERSION=${ERLANG_VERSION:-20}
BUILD_VOLUME=${BUILD_VOLUME:-mongooseim-test-build-volume}
BUILD_CONTAINER_NAME=${BUILD_CONTAINER_NAME:-mongooseim-test-builder}
RESET_DOCKER_CONTAINERS=${RESET_DOCKER_CONTAINERS:-false}
RESET_DOCKER_VOLUMES=${RESET_DOCKER_VOLUMES:-false}

if [ "$RESET_DOCKER_CONTAINERS" = true ]; then
    docker rm -f "$BUILD_CONTAINER_NAME" || true
fi

BUILD_TESTS="${BUILD_TESTS:-true}"
BUILD_MIM="${BUILD_MIM:-true}"

# DISABLE FOR NOW: we still need to run build to rsync test spec file
#if [ "$BUILD_TESTS" = false ] && [ "$BUILD_MIM" = false ]; then
#    echo "Skip starting Test Build container"
#    exit 0
#fi

# Ensure cache directory exist
mkdir -p ~/.cache/rebar3/

VARS_FILE=_build/.test_runner/$BUILD_CONTAINER_NAME-vars
mkdir -p $(dirname "$VARS_FILE")
tools/test_runner/export_test_variables.sh > "$VARS_FILE"

docker volume create --name $BUILD_VOLUME || echo "Volume creation failed"
docker run -d  \
    -v $(pwd)/$VARS_FILE:/env_vars:ro \
    -v $BUILD_VOLUME:/opt/mongooseim \
    -v $(pwd):/opt/mongooseim_src \
    -v ~/.cache/rebar3:/root/.cache/rebar3 \
    --name=$BUILD_CONTAINER_NAME \
    erlang:$ERLANG_VERSION \
    tail -F /var/log/progress || echo "Skip starting Test Build container"

# We can't just "docker volume rm", because there can be some containers still using the volume
# and docker would return an error.
if [ "$RESET_DOCKER_VOLUMES" = true ]; then
    # Remove all files on the volume $BUILD_VOLUME
    docker exec $BUILD_CONTAINER_NAME bash -c "rm -rf /opt/mongooseim/{*,.*} || true"
fi

# Builds code
docker exec -i $BUILD_CONTAINER_NAME /opt/mongooseim_src/tools/test_runner/docker-build-init.sh
