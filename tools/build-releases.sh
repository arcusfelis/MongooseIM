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

COMPILED_NODE=""
COMPILED_DEFAULT=false

function try_sync_node
{
    local NODE=$1
    if test -d _build/$NODE/lib/mongooseim/ && \
       test -f _build/$NODE/rel/mongooseim/bin/mongooseimctl; then
        if [[ "$COMPILED_DEFAULT" = false ]]; then
            # Compile default directory before syncing the first time
            COMPILED_DEFAULT=true
            ./rebar3 compile
        fi
        echo "Sync node $NODE"
        sync_node $NODE
    else
        if [  -z "$COMPILED_NODE" ]; then
            echo "Build node for the first time $NODE"
            COMPILED_NODE="$NODE"
        else
            echo "Bootstrap $NODE from $COMPILED_NODE"
            # Use mim1 as prototype for other nodes
            # Dot here ensures that it would work even if destination exists
            cp -Rp _build/$COMPILED_NODE/. _build/$NODE/
        fi
        make $NODE
    fi
}

# "make devrel", but for a list of dev nodes
if [ -z "$DEV_NODES" ] || [ "$BUILD_MIM" = false ]; then
    echo "Skip make devrel"
elif [ "$TRY_SYNC" = true ]; then
    for NODE in $DEV_NODES; do
        try_sync_node $NODE
    done
else
    echo "Build $DEV_NODES"
    make $DEV_NODES
fi

