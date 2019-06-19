#!/bin/bash
#
# Env variables:
# BUILD_TESTS - disable the script

source tools/travis-common-vars.sh

# rebar doesn't like Unicode in commit messages #1674
# So, we unset commit message env variable
# TODO: Remove it when we switch to rebar3 for big tests.
export TRAVIS_COMMIT_MESSAGE=no_message

if [ "$BUILD_TESTS" != "false" ]; then
    cd ${BASE}/big_tests && make prepare
fi

mkdir -p _build/default/lib/ejabberd_tests/ebin/
for i in tests/*_data; do
    echo "$i"
    base=$(basename "$i")
    dest="_build/default/lib/ejabberd_tests/ebin/$base"
    rm -f "$dest"
    ln -s "../../../../../$i" "$dest"
done
