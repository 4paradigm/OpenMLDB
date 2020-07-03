#!/usr/bin/env bash

ROOT_DIR=`pwd`

PROTO_BIN=$ROOT_DIR/thirdparty/bin/protoc
ulimit -c unlimited
sed -i "/protocExecutable/c\<protocExecutable>${PROTO_BIN}<\/protocExecutable>" java/pom.xml
mkdir -p java/src/main/proto/
cp -rf src/proto/tablet.proto java/src/main/proto/
cp -rf src/proto/name_server.proto java/src/main/proto/
cp -rf src/proto/common.proto java/src/main/proto/

echo "ROOT_DIR:${ROOT_DIR}"

case_xml=test_v1.xml

cd src/sdk/java/feql-auto-test-java

mvn clean test -DsuiteXmlFile=test_suite/${case_xml}