//
// Created by Chenjing on 2020-12-28.
//

#ifndef RTIDB_MINI_CLUSTER_BM_H
#define RTIDB_MINI_CLUSTER_BM_H
#include <stdio.h>

#include <string>

#include "case/sql_case.h"

enum BmRunMode { kRequestMode, kBatchRequestMode };
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}
void BM_RequestQuery(benchmark::State& state, fesql::sqlcase::SQLCase& sql_case,
                     ::rtidb::sdk::MiniCluster* mc);  // NOLINT
void BM_BatchRequestQuery(benchmark::State& state, fesql::sqlcase::SQLCase& sql_case,
                          ::rtidb::sdk::MiniCluster* mc);  // NOLINT
void MiniBenchmarkOnCase(const std::string& yaml_path, const std::string& case_id, BmRunMode engine_mode,
                         ::rtidb::sdk::MiniCluster* mc, benchmark::State* state);
#endif  // RTIDB_MINI_CLUSTER_BM_H
