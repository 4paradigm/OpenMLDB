#!/bin/bash
WORKSPACE=`pwd`

cd ${WORKSPACE}/onebox && sh start_all.sh
cd ${WORKSPACE}/java
mvn scoverage:report
TEST_SUCCESS=$?
if [[ -x "$(command -v pages)" ]]; then
    rm -rf public
    mkdir public
    cp -r ./target/site/scoverage/* public/
    pages public false
fi
exit ${TEST_SUCCESS}
