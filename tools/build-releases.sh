#!/usr/bin/env bash
# Env variable:
# - DEV_NODES - a list of releases to build
# - BUILD_MIM - can be used to disable this script
# - TRY_SYNC - speeds up recompilation of small changes.
#              Builds incorrect releases with big changes.
#
# By default all releases are built
# When DEV_NODES is empty, no releases are built

# Use bash "strict mode"
# Based on http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -eu

DEV_NODES="${DEV_NODES-mim1}"
BUILD_MIM="${BUILD_MIM-true}"
TRY_SYNC="${TRY_SYNC-false}"

# This function assumes that:
# - no release configuration has changed
# - no deps has changed
# - release has been built at least once before
function sync_node
{
    local NODE=$1
    rsync -a _build/default/lib/mongooseim/ _build/$NODE/lib/mongooseim/
}

function try_sync_node
{
    local NODE=$1
    if test -d _build/$NODE/lib/mongooseim/ && \
       test -f _build/$NODE/rel/mongooseim/bin/mongooseimctl; then
        echo "Sync node $NODE"
        sync_node $NODE
    else
        echo "Build node for the first time $NODE"
        make $NODE
    fi
}

# "make devrel", but for a list of dev nodes
if [ -z "$DEV_NODES" ] || [ "$BUILD_MIM" = false ]; then
    echo "Skip make devrel"
elif [ "$TRY_SYNC" = true ]; then
    ./rebar3 compile
    for NODE in $DEV_NODES; do
        try_sync_node $NODE
    done
else
    echo "Build $DEV_NODES"
    make $DEV_NODES
fi

