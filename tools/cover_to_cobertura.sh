#!/usr/bin/env bash
#tools/covertool -cover _build/test/cover/ct.coverdata -appname mongooseim -output cobertura.xml
tools/covertool -cover /tmp/mongoose_combined.coverdata -appname mongooseim -output cobertura.xml
