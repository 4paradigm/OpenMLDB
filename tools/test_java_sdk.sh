#!/bin/bash
WORKSPACE=`pwd`
mkdir -p build && cd build 
cmake .. && make fesql_proto fesql_parser && make fesql java_package
cd ${WORKSPACE}/onebox && sh start_all.sh
cd ${WORKSPACE}/java
mvn scoverage:report
TEST_SUCCESS=$?
cd ${WORKSPACE}/onebox && sh stop_all.sh
cd ${WORKSPACE}/java
if [[ -x "$(command -v pages)" ]]; then
    rm -rf public
    mkdir public
    cp -r ./target/site/scoverage/* public/
    pages public false
fi
exit ${TEST_SUCCESS}

