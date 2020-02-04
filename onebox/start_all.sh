#! /bin/sh
#
# start_dbms.sh
mkdir -p log/dbms
mkdir -p log/tablet
BUILD_DIR=../build
${BUILD_DIR}/src/fesql --role=dbms --tablet_endpoint=127.0.0.1:9212 --port=9211 --log_dir=./log/dbms  &
${BUILD_DIR}/src/fesql --role=tablet --port=9212 --log_dir=./log/tablet &
