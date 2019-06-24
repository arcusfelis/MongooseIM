#!/usr/bin/env bash

set -eu

echo "tools/test_runner/ensure_docker_image.sh" "$@"

function do_bootstap
{
    # Fix for "invoke-rc.d: could not determine current runlevel"
    export RUNLEVEL=1
    echo "reload yes\nprecedence ::ffff:0:0/96 100\nprecedence ::/0 10" > /etc/gai.conf
    time apt-get update
    time apt-get install -y unixodbc-dev tdsodbc simpleproxy rsync locales
    locale-gen en_US.UTF-8
}

function retry_on_timeout 
{
    local TIMEOUT=$1
    local RETRIES=$2
    shift 2

    if hash timeout ; then
        if timeout "$TIMEOUT" "$@" ; then
            echo "retry_on_timeout: success"
        elif [ "$RETRIES" = "0" ] ; then
            echo "retry_on_timeout: failed"
            exit 1
        else
            echo "retry_on_timeout: retry"
            retry_on_timeout "$TIMEOUT" "$(($RETRIES - 1))" "$@"
        fi
    else
        echo "timeout command not found, try without it"
        "$@"
    fi
}

case "$1" in
    ensure)
        # Create and bootstrap image
        ERLANG_VERSION=$2
        IMAGE=$3

        mkdir -p _build/.test_runner/docker_build
        cp ./tools/test_runner/ensure_docker_image.sh _build/.test_runner/docker_build/
        cd _build/.test_runner/docker_build

        retry_on_timeout 120s 5 docker build -t "$IMAGE" -f - . <<EOF
FROM "erlang:$ERLANG_VERSION"
ADD ensure_docker_image.sh ensure_docker_image.sh
RUN ./ensure_docker_image.sh bootstap
EOF
        ;;

    bootstap)
        do_bootstap
        ;;
esac
