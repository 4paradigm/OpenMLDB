#! /bin/sh

set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
########################################
STAGE="DEBUG"
WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"
mkdir -p $DEPS_PREFIX/lib $DEPS_PREFIX/include

export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}

cd ${DEPS_SOURCE}

if [ -f "rocksdb_succ" ]
then
    echo "rocksdb exist"
else
    echo "start install rocksdb ..."
    # rocksdb
    wget http://pkg.4paradigm.com/rtidb/dev/rocksdb-5.18.3.tar.gz >/dev/null
    tar zxf rocksdb-5.18.3.tar.gz  >/dev/null
    cd rocksdb-5.18.3
    export CPPFLAGS=-I${DEPS_PREFIX}/include
    export LDFLAGS=-L${DEPS_PREFIX}/lib
    make static_lib -j2 >/dev/null
    cp -rf ./include/* ${DEPS_PREFIX}/include
    cp librocksdb.a ${DEPS_PREFIX}/lib
    cd -
    touch rocksdb_succ
    echo "install rocksdb done"
fi

