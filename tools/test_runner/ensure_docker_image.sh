#!/usr/bin/env bash

set -eu

echo "tools/test_runner/ensure_docker_image.sh" "$@"

function do_bootstap
{
    # Fix for "invoke-rc.d: could not determine current runlevel"
    export RUNLEVEL=1
    echo "reload yes\nprecedence ::ffff:0:0/96 100\nprecedence ::/0 10" > /etc/gai.conf
    time apt-get update
    # tools/travis-publish-github-comment.sh needs jq
    time apt-get install -y unixodbc-dev tdsodbc socat rsync locales jq
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
        REPO_DIR="$(pwd)"

        # Pulling can take longer time than retry_on_timeout limit, so do it as a separate step
        time docker pull "erlang:$ERLANG_VERSION"

        # Build in a separate directory because:
        # docker -f - is broken on macosx (sometimes), it returns:
        # Error response from daemon: the Dockerfile (.dockerfile.445b96411d14ac9327de) cannot be empty

        BUILD_DIR=$(mktemp -d)
        cp ./tools/test_runner/ensure_docker_image.sh "$BUILD_DIR/"
        cd "$BUILD_DIR"

        tee "Dockerfile" <<EOF
FROM "erlang:$ERLANG_VERSION"
ADD ensure_docker_image.sh ensure_docker_image.sh
RUN ./ensure_docker_image.sh bootstap
EOF

        exit_code=0

        retry_on_timeout 120s 3 docker build -t "$IMAGE" . || exit_code=$?

        rm "Dockerfile" "ensure_docker_image.sh"

        cd "$REPO_DIR"

        rmdir "$BUILD_DIR"

        exit "$exit_code"

        ;;

    bootstap)
        do_bootstap
        ;;
esac
