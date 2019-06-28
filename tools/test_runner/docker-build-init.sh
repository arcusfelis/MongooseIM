#!/usr/bin/env bash
echo "Running from inside docker"

# Copy stdout and stderr into file, docker can use as a log
# https://unix.stackexchange.com/questions/67652/copy-stdout-and-stderr-to-a-log-file-and-leave-them-on-the-console-within-the-sc
#
# Used by tail -f command running by "docker-run"
# so, you can use "docker logs this-container" to see logs.
exec &> >(tee /var/log/progress)

# Ensure, that ROOT_SCRIPT_PID env variable is not passed from the man test-runner script.
unset ROOT_SCRIPT_PID

set -eu

# -o allexport enables all following variable definitions to be exported.
# +o allexport disables this feature.
set -o allexport
source /env_vars
set +o allexport

# Ah, envs
export HOME=/root

export TRY_SYNC=true

function install_deps
{
    echo "reload yes\nprecedence ::ffff:0:0/96 100\nprecedence ::/0 10" > /etc/gai.conf
    apt-get update
    apt-get install -y unixodbc-dev simpleproxy rsync
    touch /root/has_deps_installed
}

test -f /root/has_deps_installed || install_deps

# rsync -a src_directory/ dst_directory/

echo "Rsync code"
rsync -a \
    --exclude _build \
    --exclude big_tests/_build \
    --exclude big_tests/ct_report \
    /opt/mongooseim_src/ \
    /opt/mongooseim/

cd /opt/mongooseim/

BUILD_MIM="${BUILD_MIM:-true}"
BUILD_TESTS="${BUILD_TESTS:-true}"

if [ "$BUILD_MIM" = true ]; then
    echo "Build releases"
    tools/test_runner/time_buffered # Build time_buffered utility
    ./tools/build-releases.sh
fi

if [ "$BUILD_TESTS" = true ]; then
    echo "Build tests"
    ./tools/travis-build-tests.sh
fi
