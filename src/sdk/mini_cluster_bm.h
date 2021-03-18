/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_SDK_MINI_CLUSTER_BM_H_
#define SRC_SDK_MINI_CLUSTER_BM_H_
#include <stdio.h>

#include <memory>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "case/sql_case.h"
#include "sdk/mini_cluster.h"

enum BmRunMode { kRequestMode, kBatchRequestMode };
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}
void BM_RequestQuery(benchmark::State& state, fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                     ::fedb::sdk::MiniCluster* mc);
void BM_BatchRequestQuery(benchmark::State& state, fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                          ::fedb::sdk::MiniCluster* mc);
fesql::sqlcase::SQLCase LoadSQLCaseWithID(const std::string& yaml, const std::string& case_id);
void MiniBenchmarkOnCase(fesql::sqlcase::SQLCase& sql_case, BmRunMode engine_mode,  // NOLINT
                         ::fedb::sdk::MiniCluster* mc, benchmark::State* state);
void MiniBenchmarkOnCase(const std::string& yaml_path, const std::string& case_id, BmRunMode engine_mode,
                         ::fedb::sdk::MiniCluster* mc, benchmark::State* state);
#endif  // SRC_SDK_MINI_CLUSTER_BM_H_
