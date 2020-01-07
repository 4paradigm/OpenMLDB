#! /bin/sh
#
# micro_bench.sh

mkdir -p build && cd build 
cmake .. -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_LTO=true -DCOVERAGE_ENABLE=OFF -DTESTING_ENABLE=OFF
make -j16 engine_bm
make -j16 fesql_client_bm
make -j16 fesql_client_batch_run_bm
echo "engine benchmark"
src/vm/engine_bm 2>/dev/null

echo "fesql client one run benchmark"
src/vm/fesql_client_bm 2>/dev/null

echo "fesql client batch run benchmark"
src/vm/fesql_client_batch_run_bm 2>/dev/null

