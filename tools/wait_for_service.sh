#!/usr/bin/env bash
# We cannot just connect to 127.0.0.1:9042 because Docker is very "smart" and
# exposes ports before the service is ready

if [ "$#" -ne 2 ]; then
    exit "Illegal number of parameters"
fi

set -e

function get_container_ip
{
    local CONTAINER=$1
    local DOCKER_NETWORK=$2
    if [ "$DOCKER_NETWORK" = "default" ] || [ "$DOCKER_NETWORK" = "" ]; then
        docker inspect -f {{.NetworkSettings.IPAddress}} "$CONTAINER"
    else
        docker inspect -f '{{ $network := index .NetworkSettings.Networks "'"$DOCKER_NETWORK"'" }}{{ $network.IPAddress}}' "$CONTAINER"
    fi
}

CONTAINER="$1"
PORT="$2"
DOCKER_NETWORK="${DOCKER_NETWORK-default}"

IP=$(get_container_ip "$CONTAINER" "$DOCKER_NETWORK")
echo "$CONTAINER IP is $IP"

if [ -z "$DOCKER_NETWORK" ] || [ `uname` = "Darwin" ]; then
  # Waiter should be in the same docker network, as the container
  WAITER=wait-helper-$DOCKER_NETWORK
  # Direct access to IPs is not supported on Mac
  # https://docs.docker.com/docker-for-mac/networking/
  # But we can run wait-for-it from another container
  docker run --rm -d --network=$DOCKER_NETWORK --name $WAITER ubuntu sleep infinity || echo "We can continue if the $WAITER exists"
  docker cp tools/wait-for-it.sh $WAITER:/wait-for-it.sh
  echo "Wait for $IP:$PORT"
  docker exec $WAITER /wait-for-it.sh -h "$IP" -p "$PORT"
else
  tools/wait-for-it.sh -h "$IP" -p "$PORT"
fi
