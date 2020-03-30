#! /bin/sh
#
# start_dbms.sh
mkdir -p log/dbms
mkdir -p log/tablet
BUILD_DIR=../build
${BUILD_DIR}/src/fesql --role=dbms  --port=9211  >dbms.log 2>&1 &
${BUILD_DIR}/src/fesql --role=tablet --endpoint=127.0.0.1:9212 --port=9212 --dbms_endpoint=127.0.0.1:9211 >tablet.log 2>&1 &
