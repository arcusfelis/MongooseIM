#!/bin/bash

TOOLS=`dirname $0`

if [ `uname` = "Darwin" ]; then
    BASE=$(cd "$TOOLS/.."; pwd -P)
    # Don't forget to install gsed command using "brew install gnu-sed"
    SED=gsed
else
    BASE=`readlink -f ${TOOLS}/..`
    SED=sed
fi

TLS_DIST=${TLS_DIST:-no}
START_NODES=${START_NODES:-true}

DEFAULT_DEV_NODES="mim1 mim2 mim3 fed1 reg1"
DEV_NODES="${DEV_NODES-$DEFAULT_DEV_NODES}"

# Create a bash array DEFAULT_DEV_NODES with node names
IFS=' ' read -r -a DEV_NODES_ARRAY <<< "$DEV_NODES"

PUBLISH_PORTS=${PUBLISH_PORTS:-true}

# Example: mktempdir "PREFIX"
#
# MAC OS X and docker specific:
#   Docker for Mac limits where mounts can be.
#   Mounts can be in /tmp, /Users, /Volumes but not in /var/folders/cd/
#   Default behaviour of mktemp on Mac is to create a directory like
#   /var/folders/cd/qgvc26bj6hg1kgr41q96zydh0000gp/T/tmp.Sa9w8Xp3
function mktempdir
{
    mktemp -d "/tmp/$1.XXXXXXXXX"
}

function mount_ro_volume
{
    echo "-v $1:$2:ro"
}

function publish_port
{
    if [ "$PUBLISH_PORTS" = 'true' ]; then
        echo "-p $1:$2"
    fi
}

function container_name
{
    echo "$1$DOCKER_NAME_PREFIX"
}

function docker_service
{
    if [ ! -z "$DOCKER_NETWORK" ]; then
        echo " --network-alias=$1 --network=$DOCKER_NETWORK  --cpu-shares=128 "
    fi
}

function docker_network
{
    if [ ! -z "$DOCKER_NETWORK" ]; then
        echo " --network=$DOCKER_NETWORK "
    fi
}
