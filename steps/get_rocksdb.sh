rm ../.deps/usr/lib/librocksdb.a
rm -rf ../.deps/usr/include/rocksdb
echo "start install rocksdb ..."
wget -e use_proxy=off -O rocksdb-5.18.3.tar.gz  https://github.com/facebook/rocksdb/archive/v5.18.3.tar.gz
tar zxf rocksdb-5.18.3.tar.gz
cd rocksdb-5.18.3
# export CPPFLAGS=-I${DEPS_PREFIX}/include
# export LDFLAGS=-L${DEPS_PREFIX}/lib
CXXFLAGS='-Wno-error=deprecated-copy -Wno-error=pessimizing-move' make static_lib -j8
# todo see https://github.com/facebook/rocksdb/issues/5303 for more information about compile on gcc9
cp -rf ./include/* ../../.deps/usr/include
cp librocksdb.a ../../.deps/usr/lib
echo "install rocksdb done"