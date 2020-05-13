#!/bin/bash
mkdir -p build && cd build 
cmake .. && make fesql_proto && make fesql_parser && make java_package

cd ../java
mvn scoverage:report
TEST_SUCCESS=$?

if [[ -x "$(command -v pages)" ]]; then
    rm -rf public
    mkdir public
    cp -r ./target/site/scoverage/* public/
    pages public false
fi

exit ${TEST_SUCCESS}

