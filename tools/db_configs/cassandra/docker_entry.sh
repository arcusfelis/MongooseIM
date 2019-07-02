#!/bin/bash

password=fake_server

cat - >>"${CASSANDRA_CONFIG}/cassandra.yaml" <<-EOF

	client_encryption_options:
	    enabled:  true
	    optional: false
	    keystore: ${CASSANDRA_CONFIG}/fake_server.jks
	    keystore_password: ${password}

	EOF

openssl pkcs12 -export                                     \
               -out "${CASSANDRA_CONFIG}/fake_server.p12"  \
               -in /ssl/mongooseim/cert.pem                      \
               -inkey /ssl/mongooseim/privkey.pem                \
               -password "pass:${password}"

keytool -importkeystore                                     \
        -destkeystore "${CASSANDRA_CONFIG}/fake_server.jks" \
        -deststorepass "${password}"                        \
        -srckeystore "${CASSANDRA_CONFIG}/fake_server.p12"  \
        -srcstorepass "${password}"                         \
        -srcstoretype 'PKCS12'


# From https://github.com/saidbouras/cassandra-docker-unit/blob/master/scripts/setup-config.sh
# https://medium.com/@saidbouras/running-integration-tests-with-apache-cassandra-42305dc260a6

# Disable virtual nodes
sed -i -e "s/num_tokens/\#num_tokens/" $CASSANDRA_CONFIG/cassandra.yaml

# With virtual nodes disabled, we have to configure initial_token
sed -i -e "s/\# initial_token:/initial_token: 0/" $CASSANDRA_CONFIG/cassandra.yaml
echo "JVM_OPTS=\"\$JVM_OPTS -Dcassandra.initial_token=0\"" >> $CASSANDRA_CONFIG/cassandra-env.sh

# set 0.0.0.0 Listens on all configured interfaces
sed -i -e "s/^rpc_address.*/rpc_address: 0.0.0.0/" $CASSANDRA_CONFIG/cassandra.yaml

# Be your own seed
sed -i -e "s/- seeds: \"127.0.0.1\"/- seeds: \"$SEEDS\"/" $CASSANDRA_CONFIG/cassandra.yaml

# Disable gossip, no need in one node cluster
echo "JVM_OPTS=\"\$JVM_OPTS -Dcassandra.skip_wait_for_gossip_to_settle=0\"" >> $CASSANDRA_CONFIG/cassandra-env.sh



echo "Executing $@"
exec "$@"
