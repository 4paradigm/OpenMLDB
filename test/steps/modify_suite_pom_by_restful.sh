#!/usr/bin/env bash

CASE_XML=$1
DEPLOY_MODE=$2
FEDB_SDK_VERSION=$3
BUILD_MODE=$4
FEDB_SERVER_VERSION=$5
echo "deploy_mode:${DEPLOY_MODE}"
ROOT_DIR=`pwd`
echo "test_version:$FEDB_SDK_VERSION"
cd java/hybridsql-test/fedb-restful-test
# modify suite_xml
sed -i "s#<parameter name=\"version\" value=\".*\"/>#<parameter name=\"version\" value=\"${FEDB_SERVER_VERSION}\"/>#"  test_suite/${CASE_XML}
sed -i "s#<parameter name=\"env\" value=\".*\"/>#<parameter name=\"env\" value=\"${DEPLOY_MODE}\"/>#"  test_suite/${CASE_XML}
if [[ "${BUILD_MODE}" == "SRC" ]]; then
    sed -i "s#<parameter name=\"fedbPath\" value=\".*\"/>#<parameter name=\"fedbPath\" value=\"${ROOT_DIR}/OpenMLDB/build/bin/fedb\"/>#" test_suite/${CASE_XML}
fi
# modify pom
cd ${ROOT_DIR}
cd java/hybridsql-test/fedb-sdk-test
sed -i "s#<openmldb.version>.*</openmldb.version>#<openmldb.version>${FEDB_SDK_VERSION}</openmldb.version>#" pom.xml

cd ${ROOT_DIR}