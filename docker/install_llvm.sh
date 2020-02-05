#! /bin/sh

PLATFORM=$1
set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
# mac can't support source compile for the followings:
# 1. zookeeper_client_c
# 2. rocksdb
########################################
STAGE="DEBUG"
WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"
mkdir -p $DEPS_PREFIX/lib $DEPS_PREFIX/include
export CXXFLAGS="-O3 -fPIC"
export CFLAGS="-O3 -fPIC"
export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}

cd ${DEPS_SOURCE}

if [ -f "llvm_succ" ]
then 
    echo "llvm_exist"
else
    if [ ! -d "llvm-9.0.0.src" ]
    then
        wget --no-check-certificate -O llvm-9.0.0.src.tar.xz http://releases.llvm.org/9.0.0/llvm-9.0.0.src.tar.xz
        tar xf llvm-9.0.0.src.tar.xz
    fi
    cd llvm-9.0.0.src && mkdir -p build
    cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC .. >/dev/null
    make -j12 && make install
    cd ${DEPS_SOURCE}
    touch llvm_succ
fi

#cd ${WORK_DIR} && rm -rf ${DEPS_SOURCE}
