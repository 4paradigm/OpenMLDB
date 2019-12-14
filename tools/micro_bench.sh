#! /bin/sh
#
# micro_bench.sh

mkdir -p build && cd build 
cmake .. -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_LTO=true -DCOVERAGE_ENABLE=OFF -DTESTING_ENABLE=OFF
make fesql_proto && make -j4 engine_bm
src/vm/engine_bm 2>/dev/null


