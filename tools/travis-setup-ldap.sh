#!/usr/bin/env bash

set -e
source tools/travis-common-vars.sh
LDAP_NAME=$(container_name mongooseim-ldap)

LDAP_ROOTPASS=mongooseim_secret

LDAP_ROOT="cn=admin,dc=esl,dc=com"
LDAP_DOMAIN="esl.com"
LDAP_ORGANISATION="Erlang Solutions"

echo "configuring slapd"

LDAP_ROOT_DIR="$(mktempdir mongoose_ldap_root)"
LDAP_SCHEMAS_DIR="$LDAP_ROOT_DIR/prepopulate"

LDAP_DATA_VOLUME="$LDAP_NAME-data"
LDAP_CONFIG_VOLUME="$LDAP_NAME-config"

echo "LDAP_ROOT_DIR=$LDAP_ROOT_DIR"

mkdir -p "$LDAP_SCHEMAS_DIR"

cat > "$LDAP_SCHEMAS_DIR/init_entries.ldif" << EOL
dn: ou=Users,dc=esl,dc=com
objectClass: organizationalUnit
ou: users
EOL

docker rm -f $LDAP_NAME || echo "Skip removing previous container"
docker volume rm -f $LDAP_DATA_VOLUME $LDAP_CONFIG_VOLUME || true
docker volume create $LDAP_DATA_VOLUME
docker volume create $LDAP_CONFIG_VOLUME

# Host on non-standard higher port 3389 to avoid problems with lower ports
# Default LDAP port is 389
docker run -d \
    $(docker_service ldap) \
    --name $LDAP_NAME \
    $(publish_port 3389 389) \
    -e SLAPD_DOMAIN="$LDAP_DOMAIN" \
    -e SLAPD_PASSWORD="$LDAP_ROOTPASS" \
    -e SLAPD_ORGANIZATION="$LDAP_ORGANISATION" \
    -v "$LDAP_CONFIG_VOLUME:/etc/ldap" \
    -v "$LDAP_DATA_VOLUME:/var/lib/ldap" \
    $(mount_ro_volume "$LDAP_SCHEMAS_DIR" /etc/ldap.dist/prepopulate/) \
    openfrontier/openldap-server
