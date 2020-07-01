#!/usr/bin/env bash

ROOT_DIR=`pwd`

echo "ROOT_DIR:${ROOT_DIR}"

case_xml=test_v1.xml

cd src/sdk/feql-auto-test-java

mvn clean test -DsuiteXmlFile=test_suite/${case_xml}