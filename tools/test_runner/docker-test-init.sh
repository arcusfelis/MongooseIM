#!/usr/bin/env bash
echo "Running from inside docker"

# Copy stdout and stderr into file, docker can use as a log
# https://unix.stackexchange.com/questions/67652/copy-stdout-and-stderr-to-a-log-file-and-leave-them-on-the-console-within-the-sc
#
# Used by tail -f command running by "docker-run"
# so, you can use "docker logs this-container" to see logs.
exec &> >(tee /var/log/progress)

set -eu

# -o allexport enables all following variable definitions to be exported.
# +o allexport disables this feature.
set -o allexport
source /env_vars
set +o allexport

echo "Env variables: "
cat /env_vars
echo ""

# Ah, envs
export HOME=/root

export ROOT_SCRIPT_PID=$$
echo "ROOT_SCRIPT_PID=$ROOT_SCRIPT_PID"

# To avoid this error we need to run odbcinst:
# eodbc:connect("DSN=mongoose-mssql;UID=sa;PWD=mongooseim_secret+ESL123", []).
# {error,"[unixODBC][FreeTDS][SQL Server]Unable to connect to data source"
#        "[unixODBC][FreeTDS][SQL Server]Unknown host machine name. "
#        "SQLSTATE IS: 01000 Connection to database failed."}
odbcinst -i -s -f ~/.odbc.ini

rsync -a /opt/mongooseim_build/ /opt/mongooseim/
cd /opt/mongooseim

unset USE_DOCKER_FOR_TEST_RUNNER
export SKIP_DB_SETUP=true

function forward_port
{
    LOCAL_PORT=$1
    REMOTE_PORT=$2
    REMOTE_HOST=$3
    nohup simpleproxy -L "$LOCAL_PORT" -R "$REMOTE_HOST:$REMOTE_PORT" >runlong.out 2>runlong.err &
}

function forward_same_port
{
    forward_port $1 $1 $2
}

pkill simpleproxy || true

# Same order of entries as in tools/travis-setup-db.sh
forward_same_port 3306 mysql
forward_same_port 5432 pgsql

forward_same_port 8087 riak
forward_same_port 8098 riak

forward_same_port 9042 cassandra-proxy
forward_same_port 9191 cassandra-proxy

forward_same_port 9200 elasticsearch

forward_same_port 1433 mssql

forward_same_port 6379 redis

forward_same_port 5672 rabbitmq

forward_port 3389 389 ldap

export PERIODIC_STRING="\n"

vmstat -n 1 | while read line; do echo "$(date +'%T %s') $line"; done > /tmp/vmstat.log &

# From tools/test_runner/selected-tests-to-test-spec.sh
if [[ -f auto_small_tests.spec ]]; then
    mkdir -p _build/test/lib/mongooseim
    cp auto_small_tests.spec _build/test/lib/mongooseim/auto_small_tests.spec
fi

ret_val=0
./tools/travis-test.sh || ret_val="$?"

echo "FINISHED docker-test-init.sh"

# docker exec hangs / does not return on exit.
# Here is a workaround
# https://github.com/moby/moby/issues/33039
# trap 'kill $(jobs -p)' EXIT

exit "$ret_val"

# Close stdin.
exec 0<&-

# Close stdout.
exec 1>&-

# Close stderr.
exec 3>&-
