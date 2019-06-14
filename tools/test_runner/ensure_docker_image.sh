#!/usr/bin/env bash

function do_bootstap
{
    echo "reload yes\nprecedence ::ffff:0:0/96 100\nprecedence ::/0 10" > /etc/gai.conf
    apt-get update
    apt-get install -y unixodbc-dev tdsodbc simpleproxy rsync locales
    locale-gen en_US.UTF-8
}

case "$1" in
    ensure)
        # Create and bootstrap image
        ERLANG_VERSION=$2
        IMAGE=$3

        mkdir -p _build/.test_runner/docker_build
        cp ./tools/test_runner/ensure_docker_image.sh _build/.test_runner/docker_build/
        cd _build/.test_runner/docker_build

        docker build -t "$IMAGE" -f - . <<EOF
FROM "erlang:$ERLANG_VERSION"
ADD ensure_docker_image.sh ensure_docker_image.sh
RUN ./ensure_docker_image.sh bootstap
EOF
        ;;

    bootstap)
        do_bootstap
        ;;
esac
