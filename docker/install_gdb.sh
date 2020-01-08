#! /bin/sh

PLATFORM=$1
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
export CXXFLAGS="-O3 -fPIC"
export CFLAGS="-O3 -fPIC"
export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}

cd ${DEPS_SOURCE}
if [ -f "gdb_succ" ]
then 
    echo "gdb_exist"
else
    if [ ! -d "gdb-7.11.1.tar.gz" ]
    then
        wget --no-check-certificate -O gdb-7.11.1.tar.gz http://pkg.4paradigm.com/fesql/gdb-7.11.1.tar.gz
        tar -zxvf gdb-7.11.1.tar.gz
    fi
    cd gdb-7.11.1 && ./configure --prefix=${DEPS_PREFIX} && make -j4  && make install
    cd ${DEPS_SOURCE}
    touch gdb_succ
fi
