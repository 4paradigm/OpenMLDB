/**
 * Copyright (c) 2023 OpenMLDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_INCLUDE_VM_SQL_CTX_H_
#define HYBRIDSE_INCLUDE_VM_SQL_CTX_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "node/node_manager.h"
#include "vm/engine_context.h"

namespace zetasql {
class ParserOutput;
}

namespace hybridse {
namespace vm {

class HybridSeJitWrapper;
class ClusterJob;

struct SqlContext {
    // mode: batch|request|batch request
    ::hybridse::vm::EngineMode engine_mode;
    bool is_cluster_optimized = false;
    bool is_batch_request_optimized = false;
    bool enable_expr_optimize = false;
    bool enable_batch_window_parallelization = true;
    bool enable_window_column_pruning = false;

    // the sql content
    std::string sql;
    // the database
    std::string db;

    std::unique_ptr<zetasql::ParserOutput> ast_node;
    // the logical plan
    ::hybridse::node::PlanNodeList logical_plan;
    ::hybridse::vm::PhysicalOpNode* physical_plan = nullptr;

    std::shared_ptr<hybridse::vm::ClusterJob> cluster_job;
    // TODO(wangtaize) add a light jit engine
    // eg using bthead to compile ir
    hybridse::vm::JitOptions jit_options;
    std::shared_ptr<hybridse::vm::HybridSeJitWrapper> jit = nullptr;
    Schema schema;
    Schema request_schema;
    std::string request_db_name;
    std::string request_name;
    Schema parameter_types;
    uint32_t row_size;
    uint32_t limit_cnt = 0;
    std::string ir;
    std::string logical_plan_str;
    std::string physical_plan_str;
    std::string encoded_schema;
    std::string encoded_request_schema;
    ::hybridse::node::NodeManager nm;
    ::hybridse::udf::UdfLibrary* udf_library = nullptr;

    ::hybridse::vm::BatchRequestInfo batch_request_info;

    std::shared_ptr<const std::unordered_map<std::string, std::string>> options;

    // [ALPHA] SQL diagnostic infos
    // not standardized, only index hints, no error, no warning, no other hint/info
    std::shared_ptr<IndexHintHandler> index_hints;

    SqlContext();
    ~SqlContext();
};

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_INCLUDE_VM_SQL_CTX_H_
