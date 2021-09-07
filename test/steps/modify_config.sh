#!/usr/bin/env bash

CASE_XML=$1
if [[ "${CASE_XML}" == "" ]]; then
    CASE_XML="test_all.xml"
fi
DEPLOY_MODE=$2
if [[ "${DEPLOY_MODE}" == "" ]]; then
    DEPLOY_MODE="cluster"
fi
echo "deploy_mode:${DEPLOY_MODE}"
ROOT_DIR=`pwd`
source steps/read_properties.sh
echo "test_version:$test_version"
cd java/hybridsql-test/fedb-sdk-test
system_type=`uname`
if [ ${system_type} == "Linux" ] ;then
    sed -i "s#<parameter name=\"version\" value=\".*\"/>#<parameter name=\"version\" value=\"${test_version}\"/>#"  test_suite/${CASE_XML}
else
    sed -i "" "s#<parameter name=\"version\" value=\".*\"/>#<parameter name=\"version\" value=\"${test_version}\"/>#" test_suite/${CASE_XML}
fi
if [ ${system_type} == "Linux" ] ;then
    sed -i "s#<parameter name=\"env\" value=\".*\"/>#<parameter name=\"env\" value=\"${DEPLOY_MODE}\"/>#"  test_suite/${CASE_XML}
else
    sed -i "" "s#<parameter name=\"env\" value=\".*\"/>#<parameter name=\"env\" value=\"${DEPLOY_MODE}\"/>#" test_suite/${CASE_XML}
fi
cd ${ROOT_DIR}