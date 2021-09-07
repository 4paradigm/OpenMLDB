#!/usr/bin/env bash

ROOT_DIR=`pwd`

javaSDKVersion=`cat fedb/src/sdk/java/pom.xml | grep "<version>.*</version>" | head -1 | sed 's#.*<version>\(.*\)</version>.*#\1#'`
echo "javaSDKVersion:${javaSDKVersion}"
sed -i "s#<sql.version>.*</sql.version>#<sql.version>${javaSDKVersion}</sql.version>#" java/hybridsql-test/fedb-sdk-test/pom.xml

sed -i "s#<parameter name=\"fedbPath\" value=\".*\"/>#<parameter name=\"fedbPath\" value=\"${ROOT_DIR}/fedb/build/bin/fedb\"/>#" test_suite/${CASE_XML}
