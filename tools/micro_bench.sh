#!/bin/bash
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# micro_bench.sh

set -eE

# goto toplevel directory
cd "$(dirname "$0")/.."

source tools/init_env.profile.sh


if uname -a | grep -q Darwin; then
    # in case coreutils not install on mac
    alias nproc='sysctl -n hw.logicalcpu'
fi

rm -rf build
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCOVERAGE_ENABLE=OFF -DTESTING_ENABLE=OFF -DBENCHMARK_ENABLE=ON
make -j"$(nproc)" hybridse_bm toydb_bm

echo "udf benchmark:"
src/benchmark/udf_bm 2>/dev/null

echo "toydb storage benchmark:"
examples/toydb/src/bm/storage_bm 2>/dev/null

echo "toydb engine benchmark:"
examples/toydb/src/bm/engine_bm 2>/dev/null

echo "toydb client batch run benchmark:"
examples/toydb/src/bm/hybridse_client_batch_run_bm 2>/dev/null

echo "toydb batch request benchmark:"
examples/toydb/src/bm/batch_request_bm 2>/dev/null

