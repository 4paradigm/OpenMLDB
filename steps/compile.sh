#! /bin/sh
#
# compile.sh
set -e

WORK_DIR=`pwd`
if [ "$1" = "DEBUG" ]
then
    sed -i '/set(CMAKE_BUILD_TYPE/c\set(CMAKE_BUILD_TYPE Debug)' CMakeLists.txt 
else
    sed -i '/set(CMAKE_BUILD_TYPE/c\set(CMAKE_BUILD_TYPE RelWithDebInfo)' CMakeLists.txt 
fi

VERSION=$(git tag --points-at HEAD)
VERSION=${VERSION:1}
if [ -n "${VERSION}" ]; then
    if [[ ! ($VERSION =~ ^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}$) ]]; then
        echo "$VERSION is not release version"
        exit 1
    fi
    sh ./steps/release.sh ${VERSION}
fi

sh steps/gen_code.sh

mkdir -p $WORK_DIR/build  || :
cd $WORK_DIR/build && cmake .. && make -j28
code=$?
cd $WORK_DIR
exit $code
