#!/usr/bin/env bash

# Environment variable DB is used by this script.
# If DB is undefined, than this script does nothing.

# Docker for Mac should be used on Mac (not docker-machine!)
# https://store.docker.com/editions/community/docker-ce-desktop-mac

# Other environment variables:
# PUBLISH_PORTS (default: true) - publish ports to a host
# DOCKER_NAME_PREFIX (default: "") - add prefix to each container name
# DOCKER_NETWORK (default: "") - if not empty, attach container to the network
#                                and assing DB alias

# Cleanup of volumes design
# -------------------------
#
# You want to do:
# docker inspect IMAGE_NAME -f '{{ .Config.Volumes }}'
# for each image and add name for each volume in the configuration.
#
# For example:
# 
# docker inspect postgres -f '{{ .Config.Volumes }}'
# map[/var/lib/postgresql/data:{}]
# 
# Means that we need one named volume for postgres.
set -e
TOOLS=`dirname $0`

source tools/travis-common-vars.sh

MIM_PRIV_DIR=${BASE}/priv

MYSQL_DIR=/etc/mysql/conf.d

PGSQL_ODBC_CERT_DIR=~/.postgresql

SSLDIR=${BASE}/${TOOLS}/ssl

# Don't need it for travis for speed up
RM_FLAG=" --rm "
if [ "$TRAVIS" = 'true' ]; then
    echo "Disable --rm flag on Travis"
    RM_FLAG=""
fi

echo "DOCKER_NAME_PREFIX is $DOCKER_NAME_PREFIX"
echo "PUBLISH_PORTS is $PUBLISH_PORTS"
echo "DOCKER_NETWORK is $DOCKER_NETWORK"

# Default cassandra version
CASSANDRA_VERSION=${CASSANDRA_VERSION:-3.9}

# Default ElasticSearch version
ELASTICSEARCH_VERSION=${ELASTICSEARCH_VERSION:-5.6.9}

# There is one odbc.ini for both mssql and pgsql
# Allows to run both in parallel
function install_odbc_ini
{
# CLIENT OS CONFIGURING STUFF
#
# Be aware, that underscore in TDS_Version is required.
# It can't be just "TDS Version = 7.1".
#
# To check that connection works use:
#
# {ok, Conn} = odbc:connect("DSN=mongoose-mssql;UID=sa;PWD=mongooseim_secret+ESL123",[]).
#
# To check that TDS version is correct, use:
#
# odbc:sql_query(Conn, "select cast(1 as bigint)").
#
# It should return:
# {selected,[[]],[{"1"}]}
#
# It should not return:
# {selected,[[]],[{1.0}]}
#
# Be aware, that Driver and Setup values are for Ubuntu.
# CentOS would use different ones.
cp $TOOLS/db_configs/odbc.ini ~/.odbc.ini
}

# Stores all the data needed by the container
SQL_ROOT_DIR="$(mktempdir mongoose_sql_root)"
echo "SQL_ROOT_DIR is $SQL_ROOT_DIR"

# A directory, that contains resources that needed to bootstrap a container
# i.e. certificates and config files
SQL_TEMP_DIR="$SQL_ROOT_DIR/temp"

# A directory, that contains database server data files
# It's good to keep it outside of a container, on a volume
SQL_DATA_DIR="$SQL_ROOT_DIR/data"
mkdir -p "$SQL_TEMP_DIR" "$SQL_DATA_DIR"

function setup_db(){
db=${1:-none}
echo "Setting up db: $db"
DB_CONF_DIR=${BASE}/${TOOLS}/db_configs/$db


if [ "$db" = 'mysql' ]; then
    echo "Configuring mysql"
    MYSQL_NAME=$(container_name mongooseim-mysql)
    MYSQL_DATA_VOLUME=${MYSQL_NAME}-data
    # TODO We should not use sudo
    sudo -n service mysql stop || echo "Failed to stop mysql"
    docker rm -f "$MYSQL_NAME" || echo "Skip removing previous container"
    docker volume rm -f "$MYSQL_DATA_VOLUME" || true
    docker volume create "$MYSQL_DATA_VOLUME"
    cp ${SSLDIR}/mongooseim/cert.pem ${SQL_TEMP_DIR}/fake_cert.pem
    openssl rsa -in ${SSLDIR}/mongooseim/key.pem -out ${SQL_TEMP_DIR}/fake_key.pem
    chmod a+r ${SQL_TEMP_DIR}/fake_key.pem
    # mysql_native_password is needed until mysql-otp implements caching-sha2-password
    # https://github.com/mysql-otp/mysql-otp/issues/83
    docker run -d \
        $(docker_service $db) \
        -e SQL_TEMP_DIR=/tmp/sql \
        -e MYSQL_ROOT_PASSWORD=secret \
        -e MYSQL_DATABASE=ejabberd \
        -e MYSQL_USER=ejabberd \
        -e MYSQL_PASSWORD=mongooseim_secret \
        $(mount_ro_volume ${DB_CONF_DIR}/mysql.cnf ${MYSQL_DIR}/mysql.cnf) \
        $(mount_ro_volume ${MIM_PRIV_DIR}/mysql.sql /docker-entrypoint-initdb.d/mysql.sql) \
        $(mount_ro_volume ${BASE}/${TOOLS}/docker-setup-mysql.sh /docker-entrypoint-initdb.d/docker-setup-mysql.sh) \
        $(mount_ro_volume ${SQL_TEMP_DIR} /tmp/sql) \
        -v $MYSQL_DATA_VOLUME:/var/lib/mysql \
        --health-cmd='mysqladmin ping --silent' \
        $(publish_port 3306 3306) \
        --name=$MYSQL_NAME \
        mysql --default-authentication-plugin=mysql_native_password

elif [ "$db" = 'pgsql' ]; then
    # If you see "certificate verify failed" error in Mongoose logs, try:
    # Inside tools/ssl/:
    # make clean && make
    # Than rerun the script to create a new docker container.
    echo "Configuring postgres with SSL"
    PG_NAME=$(container_name mongooseim-pgsql)
    PG_DATA_VOLUME=${PG_NAME}-data
    sudo -n service postgresql stop || echo "Failed to stop psql"
    docker rm -f "$PG_NAME" || echo "Skip removing previous container"
    docker volume rm -f "$PG_DATA_VOLUME" || true
    docker volume create "$PG_DATA_VOLUME"
    cp ${SSLDIR}/mongooseim/cert.pem ${SQL_TEMP_DIR}/fake_cert.pem
    cp ${SSLDIR}/mongooseim/key.pem ${SQL_TEMP_DIR}/fake_key.pem
    cp ${DB_CONF_DIR}/postgresql.conf ${SQL_TEMP_DIR}/.
    cp ${DB_CONF_DIR}/pg_hba.conf ${SQL_TEMP_DIR}/.
    cp ${MIM_PRIV_DIR}/pg.sql ${SQL_TEMP_DIR}/.
    docker run -d \
           $(docker_service $db) \
           -e SQL_TEMP_DIR=/tmp/sql \
           $(mount_ro_volume ${SQL_TEMP_DIR} /tmp/sql) \
           $(mount_ro_volume ${BASE}/${TOOLS}/docker-setup-postgres.sh /docker-entrypoint-initdb.d/docker-setup-postgres.sh) \
           $(publish_port 5432 5432) \
           -v $PG_DATA_VOLUME:/var/lib/postgresql/data \
           --name=$PG_NAME \
           postgres
    mkdir -p ${PGSQL_ODBC_CERT_DIR}
    cp ${SSLDIR}/ca/cacert.pem ${PGSQL_ODBC_CERT_DIR}/root.crt
    install_odbc_ini

elif [ "$db" = 'riak' ]; then
    echo "Configuring Riak with SSL"
    RIAK_NAME=$(container_name mongooseim-riak)
    # Riak volumes are defined in the Dockerfile
    # Riak data volume can be several gigabytes, so we want to clean them up by name
    # after each database setup.
    RIAK_DATA_VOLUME=${RIAK_NAME}-data
    RIAK_LOGS_VOLUME=${RIAK_NAME}-logs
    docker rm -f $RIAK_NAME || echo "Skip removing previous container"
    docker volume rm -f $RIAK_DATA_VOLUME $RIAK_LOGS_VOLUME || true
    docker volume create "$RIAK_DATA_VOLUME"
    docker volume create "$RIAK_LOGS_VOLUME"
    # Instead of docker run, use "docker create" + "docker start".
    # So we can prepare our container.
    # We use HEALTHCHECK here, check "docker ps" to get healthcheck status.
    # We can't use volumes for riak.conf, because:
    # - we want to change it
    # - container starting code runs sed on it and gets IO error,
    #   if it's a volume
    time docker create \
        $(docker_service $db) \
        $(publish_port 8087 8087) \
        $(publish_port 8098 8098) \
        -e DOCKER_RIAK_BACKEND=leveldb \
        -e DOCKER_RIAK_CLUSTER_SIZE=1 \
        --name=$RIAK_NAME \
        $(mount_ro_volume "${DB_CONF_DIR}/advanced.config" "/etc/riak/advanced.config") \
        $(mount_ro_volume "${SSLDIR}/mongooseim/cert.pem" "/etc/riak/cert.pem") \
        $(mount_ro_volume "${SSLDIR}/mongooseim/key.pem" "/etc/riak/key.pem") \
        $(mount_ro_volume "${SSLDIR}/ca/cacert.pem" "/etc/riak/ca/cacertfile.pem") \
        -v $RIAK_DATA_VOLUME:/var/lib/riak \
        -v $RIAK_LOGS_VOLUME:/var/log/riak \
        --health-cmd='riak-admin status' \
        "michalwski/docker-riak:1.0.6"
    # Use a temporary file to store config
    TEMP_RIAK_CONF=$(mktemp)
    # Export config from a container
    docker cp "$RIAK_NAME:/etc/riak/riak.conf" "$TEMP_RIAK_CONF"
    # Enable search
    $SED -i "s/^search = \(.*\)/search = on/" "$TEMP_RIAK_CONF"
    # Enable ssl by appending settings from riak.conf.ssl
    cat "${DB_CONF_DIR}/riak.conf.ssl" >> "$TEMP_RIAK_CONF"
    # Import config back into container
    docker cp "$TEMP_RIAK_CONF" "$RIAK_NAME:/etc/riak/riak.conf"
    # Erase temporary config file
    rm "$TEMP_RIAK_CONF"
    docker start $RIAK_NAME
    echo "Waiting for docker healthcheck"
    echo ""
    tools/wait_for_healthcheck.sh $RIAK_NAME
    echo "Waiting for a listener to appear"
    tools/wait_for_service.sh $RIAK_NAME 8098
    # Use riak-admin from inside the container
    export RIAK_ADMIN="docker exec $RIAK_NAME riak-admin"
    export CURL_COMMAND="docker exec $RIAK_NAME curl"
    tools/setup_riak
    # Use this command to read Riak's logs if something goes wrong
    # docker exec -it $RIAK_NAME bash -c 'tail -f /var/log/riak/*'

elif [ "$db" = 'cassandra' ]; then
    CASSANDRA_NAME=$(container_name mongooseim-cassandra)
    CASSANDRA_PROXY_NAME=$(container_name mongooseim-cassandra-proxy)
    CASSANDRA_DATA_VOLUME=${CASSANDRA_NAME}-data
    docker image pull cassandra:${CASSANDRA_VERSION}
    docker rm -f \
        $CASSANDRA_NAME \
        $CASSANDRA_PROXY_NAME || echo "Skip removing previous container"

    docker volume rm -f "$CASSANDRA_DATA_VOLUME" || true
    docker volume create "$CASSANDRA_DATA_VOLUME"

    opts="$(docker inspect -f '{{range .Config.Entrypoint}}{{println}}{{.}}{{end}}' cassandra:${CASSANDRA_VERSION})"
    opts+="$(docker inspect -f '{{range .Config.Cmd}}{{println}}{{.}}{{end}}' cassandra:${CASSANDRA_VERSION})"
    while read -r line; do
        if [ ! -z "$line" ]; then
             init_opts+=("$line")
        fi
    done <<<"$opts"
    echo -e "cassandra startup cmd:\n\t${init_opts[@]}"

    docker_entry="${DB_CONF_DIR}/docker_entry.sh"

    MIM_SCHEMA=$(pwd)/priv/cassandra.cql
    TEST_SCHEMA=$(pwd)/big_tests/tests/mongoose_cassandra_SUITE_data/schema.cql

    docker run -d                                \
               $(docker_service $db)             \
               -e MAX_HEAP_SIZE=512M             \
               -e HEAP_NEWSIZE=128M              \
               $(mount_ro_volume "$MIM_SCHEMA" "/schemas/mim.cql") \
               $(mount_ro_volume "$TEST_SCHEMA" "/schemas/test.cql") \
               $(mount_ro_volume "${SSLDIR}" "/ssl") \
               $(mount_ro_volume "${docker_entry}" "/entry.sh") \
               --name=$CASSANDRA_NAME \
               --entrypoint "/entry.sh"          \
               -v $CASSANDRA_DATA_VOLUME:/var/lib/cassandra \
               cassandra:${CASSANDRA_VERSION}    \
               "${init_opts[@]}"
    tools/wait_for_service.sh $CASSANDRA_NAME 9042 || docker logs $CASSANDRA_NAME

    # Start TCP proxy
    CASSANDRA_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $CASSANDRA_NAME)
    echo "Connecting TCP proxy to Cassandra on $CASSANDRA_IP..."
    cp ${DB_CONF_DIR}/proxy/zazkia-routes.json "$SQL_TEMP_DIR/"
    $SED -i "s/\"service-hostname\": \".*\"/\"service-hostname\": \"$CASSANDRA_IP\"/g" "$SQL_TEMP_DIR/zazkia-routes.json"
    docker run -d                               \
               $(docker_service cassandra-proxy)         \
               $(publish_port 9042 9042)                 \
               $(publish_port 9191 9191)                 \
               $(mount_ro_volume "$SQL_TEMP_DIR" /data)  \
               $(mount_ro_volume "$SQL_TEMP_DIR" /data)  \
               --name=$CASSANDRA_PROXY_NAME \
               emicklei/zazkia:0.6
    tools/wait_for_service.sh $CASSANDRA_PROXY_NAME 9042 || docker logs $CASSANDRA_PROXY_NAME

    CQLSH_DEBUG=""
    if [ "${VERBOSE:-0}" = "1" ]; then
        CQLSH_DEBUG=" --debug "
    fi

    function cqlsh
    {
        docker exec \
        -e SSL_CERTFILE=/ssl/ca/cacert.pem \
        "$CASSANDRA_NAME" \
        cqlsh "127.0.0.1" --ssl $CQLSH_DEBUG "$@"
    }

    while ! cqlsh -e 'describe cluster' ; do
        echo "Waiting for cassandra"
        sleep 1
    done

    # Apply schemas
    echo "Apply Cassandra schema"
    # For some reason, "cqlsh -f" does not create schema and no error is reported.
    cqlsh -e "source '/schemas/mim.cql'"
    cqlsh -e "source '/schemas/test.cql'"
    echo "Verify Cassandra schema"
    # Would fail with reason and exit code 2:
    # <stdin>:1:InvalidRequest: Error from server: code=2200 [Invalid query] message="unconfigured table mam_config"
    cqlsh -e "select * from mongooseim.mam_config;"
    echo "Cassandra setup done"

elif [ "$db" = 'elasticsearch' ]; then
    ELASTICSEARCH_IMAGE=docker.elastic.co/elasticsearch/elasticsearch:$ELASTICSEARCH_VERSION
    ELASTICSEARCH_PORT=9200
    ELASTICSEARCH_NAME=$(container_name mongooseim-elasticsearch)

    echo $ELASTICSEARCH_IMAGE
    docker image pull $ELASTICSEARCH_IMAGE
    docker rm -f  $ELASTICSEARCH_NAME || echo "Skip removing previous container"

    echo "Starting ElasticSearch $ELASTICSEARCH_VERSION from Docker container"
    docker run -d $RM_FLAG \
           $(docker_service $db) \
           $(publish_port $ELASTICSEARCH_PORT 9200) \
           -e "http.host=0.0.0.0" \
           -e "transport.host=127.0.0.1" \
           -e "xpack.security.enabled=false" \
           -e "ES_JAVA_OPTS=-Xmx521m -Xms256m" \
           --name $ELASTICSEARCH_NAME \
           $ELASTICSEARCH_IMAGE
    echo "Waiting for ElasticSearch to start listening on port"
    tools/wait_for_service.sh $ELASTICSEARCH_NAME $ELASTICSEARCH_PORT || docker logs $ELASTICSEARCH_NAME

    ELASTICSEARCH_URL=http://localhost:$ELASTICSEARCH_PORT
    ELASTICSEARCH_PM_MAPPING="$(pwd)/priv/elasticsearch/pm.json"
    ELASTICSEARCH_MUC_MAPPING="$(pwd)/priv/elasticsearch/muc.json"
    ELASTICSEARCH_PM_MAPPING_DATA=$(cat "$ELASTICSEARCH_PM_MAPPING")
    ELASTICSEARCH_MUC_MAPPING_DATA=$(cat "$ELASTICSEARCH_MUC_MAPPING")

    # Wait for ElasticSearch endpoint before applying bindings
    for i in 1..30; do
        if docker exec $ELASTICSEARCH_NAME curl $ELASTICSEARCH_URL ; then
            break
        fi
        echo "Waiting for ElasticSearch $i"
        sleep 1
    done

    echo "Putting ElasticSearch mappings"
    (docker exec $ELASTICSEARCH_NAME \
        curl -X PUT $ELASTICSEARCH_URL/messages -d "$ELASTICSEARCH_PM_MAPPING_DATA" -w "status: %{http_code}" | grep "status: 200" > /dev/null) || \
        (echo "Failed to put PM mapping into ElasticSearch" && exit 1)
    (docker exec $ELASTICSEARCH_NAME \
        curl -X PUT $ELASTICSEARCH_URL/muc_messages -d "$ELASTICSEARCH_MUC_MAPPING_DATA" -w "status: %{http_code}" | grep "status: 200" > /dev/null) || \
        (echo "Failed to put MUC mapping into ElasticSearch" && exit 1)

elif [ "$db" = 'mssql' ]; then
    # LICENSE STUFF, IMPORTANT
    #
    # SQL Server Developer edition
    # http://download.microsoft.com/download/4/F/7/4F7E81B0-7CEB-401D-BCFA-BF8BF73D868C/EULAs/License_Dev_Linux.rtf
    #
    # Information from that license:
    # > a. General.
    # > You may install and use copies of the software on any device,
    # > including third party shared devices, to design, develop, test and
    # > demonstrate your programs.
    # > You may not use the software on a device or server in a
    # > production environment.
    #
    # > We collect data about how you interact with this software.
    #   READ MORE...
    #
    # > BENCHMARK TESTING.
    # > You must obtain Microsoft's prior written approval to disclose to
    # > a third party the results of any benchmark test of the software.

    # SCRIPTING STUFF
    MSSQL_NAME=$(container_name mongooseim-mssql)
    docker rm -f $MSSQL_NAME || echo "Skip removing previous container"
    docker volume rm -f $MSSQL_NAME-data || echo "Skip removing previous volume"
    #
    # MSSQL wants secure passwords
    # i.e. just "mongooseim_secret" would not work.
    #
    # We don't overwrite --entrypoint, but it's possible.
    # It has no '/docker-entrypoint-initdb.d/'-like interface.
    # So we would put schema into some random place and
    # apply it inside 'docker-exec' command.
    #
    # ABOUT VOLUMES
    # Just using /var/opt/mssql volume is not enough.
    # We need mssql-data-volume.
    #
    # Both on Mac and Linux
    # https://github.com/Microsoft/mssql-docker/issues/12
    #
    # Otherwise we get an error in logs
    # Error 87(The parameter is incorrect.) occurred while opening file '/var/opt/mssql/data/master.mdf'
    #
    # MSSQL requires at least 2GB to run. But don't let it to grab more.
    docker run -d $(publish_port 1433 1433)                     \
               $(docker_service $db)                            \
               --name=$MSSQL_NAME                               \
               -e "ACCEPT_EULA=Y"                               \
               -e "SA_PASSWORD=mongooseim_secret+ESL123"        \
               -e "MSSQL_MEMORY_LIMIT_MB=2048"                  \
               $(mount_ro_volume "$(pwd)/priv/mssql2012.sql" "/mongoose.sql")  \
               -v $MSSQL_NAME-data:/var/opt/mssql/data \
               --health-cmd='/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "mongooseim_secret+ESL123" -Q "SELECT 1"' \
               microsoft/mssql-server-linux
    tools/wait_for_healthcheck.sh $MSSQL_NAME
    tools/wait_for_service.sh $MSSQL_NAME 1433

    docker exec $MSSQL_NAME \
        /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "mongooseim_secret+ESL123" \
        -Q "CREATE DATABASE ejabberd"
    docker exec $MSSQL_NAME \
        /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "mongooseim_secret+ESL123" \
        -Q "ALTER DATABASE ejabberd SET READ_COMMITTED_SNAPSHOT ON"
    docker exec $MSSQL_NAME \
        /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "mongooseim_secret+ESL123" \
        -i mongoose.sql

    install_odbc_ini

elif [ "$db" = 'redis' ]; then
    REDIS_NAME=$(container_name mongooseim-redis)
    REDIS_DATA_VOLUME=${REDIS_NAME}-data
    docker rm -f "$REDIS_NAME" || echo "Skip removing the previous container"
    docker volume rm -f "$REDIS_DATA_VOLUME" || true
    docker volume create "$REDIS_DATA_VOLUME"
    docker run -d --name "$REDIS_NAME" \
        $(docker_service $db) \
        $(publish_port 6379 6379) \
        -v $REDIS_DATA_VOLUME:/data \
        --health-cmd='redis-cli -h "127.0.0.1" ping' \
        redis

elif [ "$db" = 'ldap' ]; then
    tools/travis-setup-ldap.sh

elif [ "$db" = 'rabbitmq' ]; then
    RABBIT_NAME=$(container_name mongooseim-rabbitmq)
    RABBIT_DATA_VOLUME=${RABBIT_NAME}-data
    docker rm -f "$RABBIT_NAME" || echo "Skip removing the previous container"
    docker volume rm -f "$RABBIT_DATA_VOLUME" || true
    docker volume create "$RABBIT_DATA_VOLUME"
    
    docker run -d --name $RABBIT_NAME \
        -v $RABBIT_DATA_VOLUME:/var/lib/rabbitmq \
        $(docker_service $db) \
        $(publish_port 5672 5672) \
        rabbitmq:3.7

else
    echo "Skip setting up database"
fi
}

# The DB env var may contain single value "mysql"
# or list of values separated with a space "elasticsearch cassandra"
# in case of list of values all listed database will be set up (or at least tried)

for db in ${DB}; do
    setup_db $db
done
