#!/bin/bash

source tools/travis-common-vars.sh

cd ${BASE}/test.disabled/ejabberd_tests && make prepare

