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
curl ftp://ftp.4pd.io/pub/team_hpc/titanse/pz-titanse-v0.1.1-release.tar.gz -o pz.tar.gz
tar zxf pz.tar.gz -C ./thirdparty

curl ftp://ftp.4pd.io/pub/team_hpc/titanse/rocksdb-titanse-v0.1.1-release.tar.gz -o rocksdb.tar.gz
tar zxf rocksdb.tar.gz -C ./thirdparty
curl "https://nexus.4pd.io/repository/raw-hosted/fpga-rte/release/v0.2.1/aclrte-linux64-aclrte-19.2.0.57-v0.2.1-release.tar.gz" -O
tar zxf aclrte-linux64-aclrte-19.2.0.57-v0.2.1-release.tar.gz -C ./thirdparty
sed -i 's/\"Enable pz compression for ssd tables\"\ OFF/\"Enable pz compression for ssd tables\"\ ON/g' CMakeLists.txt
sh steps/gen_code.sh

mkdir -p $WORK_DIR/build  || :
cd $WORK_DIR/build && cmake .. && make -j5
code=$?
cd $WORK_DIR
exit $code
