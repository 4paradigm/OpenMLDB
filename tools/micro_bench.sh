#! /bin/sh
#
# micro_bench.sh
PWD=`pwd`
export JAVA_HOME=${PWD}/thirdparty/jdk1.8.0_141
export PATH=${PWD}/thirdparty/bin:$JAVA_HOME/bin:${PWD}/thirdparty/apache-maven-3.6.3/bin:$PATH

mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCOVERAGE_ENABLE=OFF -DTESTING_ENABLE=ON
make -j16 fesql_bm

echo "storage benchmark:"
src/bm/storage_bm 2>/dev/null

echo "udf benchmark:"
src/bm/udf_bm 2>/dev/null

echo "runner benchmark:"
src/bm/runner_bm 2>/dev/null

echo "engine benchmark:"
src/bm/engine_bm 2>/dev/null

echo "fesql client batch run benchmark:"
src/bm/fesql_client_batch_run_bm 2>/dev/null

echo "fesql batch request benchmark:"
src/bm/batch_request_bm 2>/dev/null

