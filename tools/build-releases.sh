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
TRY_FAST="${TRY_FAST-false}"

# This function assumes that:
# - no release configuration has changed
# - no deps has changed
# - release has been built at least once before
function sync_node
{
    local NODE=$1
    rsync -a --exclude lib/mongooseim/.rebar3/erlcinfo \
        _build/default/lib/mongooseim/ _build/$NODE/lib/mongooseim/
}

COMPILED_DEFAULT=false
FIRST_NODE=

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
        make $NODE
    fi
}

function try_fast_node
{
    local NODE=$1

    if [[ "$FIRST_NODE" = "" ]]; then
        FIRST_NODE=$NODE
        make $NODE
    else
        # Clone from first node
        rsync -a _build/$FIRST_NODE/ _build/$NODE/

        mkdir -p _build/.test_runner/
        cp rel/$NODE.vars.config _build/.test_runner/dev.vars.config
        ( cd tools/overlay_project/; ../../rebar3 release )

        rsync -a tools/overlay_project/_build/default/rel/mongooseim/_prefix/  _build/$NODE/rel/mongooseim/
    fi
}

# "make devrel", but for a list of dev nodes
if [ -z "$DEV_NODES" ] || [ "$BUILD_MIM" = false ]; then
    echo "Skip make devrel"
elif [ "$TRY_SYNC" = true ]; then
    for NODE in $DEV_NODES; do
        time try_sync_node $NODE
    done
elif [ "$TRY_FAST" = true ]; then
    for NODE in $DEV_NODES; do
        time try_fast_node $NODE
    done
else
    echo "Build $DEV_NODES"
    make $DEV_NODES
fi

