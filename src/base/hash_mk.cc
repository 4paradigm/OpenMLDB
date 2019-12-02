/*
 * hash_mk.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "base/hash.h"
#include "benchmark/benchmark.h"

static void BM_HashFunction(benchmark::State& state) { //NOLINT
    for (auto _ : state) {
        int32_t i = -1;
        benchmark::DoNotOptimize(
            ::fesql::base::MurmurHash64A(&i, 4, 0xe17a1465));
    }
}

BENCHMARK(BM_HashFunction);
BENCHMARK_MAIN();
