/*
 * fesql_run_engine.cc
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
#include <utility>
#include "vm/engine_test.h"

DEFINE_string(yaml_path, "", "Yaml filepath to load cases from");
DEFINE_string(runner_mode, "batch",
              "Specify runner mode, can be batch or request");
DEFINE_string(cluster_mode, "standalone",
              "Specify cluster mode, can be standalone or cluster");
DEFINE_bool(
    enable_batch_request_opt, true,
    "Specify whether perform batch request optimization in batch request mode");
DEFINE_int32(run_iters, 0, "Measure the approximate run time if specified");
DEFINE_int32(case_id, -1, "Specify the case id to run and skip others");

namespace fesql {
namespace vm {

int DoRunEngine(const SQLCase& sql_case, const EngineOptions& options,
                EngineMode engine_mode) {
    std::shared_ptr<EngineTestRunner> runner;
    if (engine_mode == kBatchMode) {
        runner = std::make_shared<BatchEngineTestRunner>(sql_case, options);
    } else if (engine_mode == kRequestMode) {
        runner = std::make_shared<RequestEngineTestRunner>(sql_case, options);
    } else {
        runner = std::make_shared<BatchRequestEngineTestRunner>(
            sql_case, options, sql_case.batch_request().common_column_indices_);
    }
    if (FLAGS_run_iters > 0) {
        runner->RunBenchmark(FLAGS_run_iters);
    } else {
        runner->RunCheck();
    }
    return runner->return_code();
}

int RunSingle(const std::string& yaml_path) {
    std::vector<SQLCase> cases;
    if (!SQLCase::CreateSQLCasesFromYaml("", yaml_path, cases)) {
        LOG(WARNING) << "Load cases from " << yaml_path << " failed";
        return ENGINE_TEST_RET_INVALID_CASE;
    }
    EngineOptions options;
    options.set_cluster_optimized(FLAGS_cluster_mode == "cluster");
    options.set_batch_request_optimized(FLAGS_enable_batch_request_opt);
    for (auto& sql_case : cases) {
        if (FLAGS_case_id >= 0 &&
            std::to_string(FLAGS_case_id) != sql_case.id()) {
            continue;
        }
        EngineMode mode;
        if (FLAGS_runner_mode == "batch") {
            mode = kBatchMode;
        } else if (FLAGS_runner_mode == "request") {
            mode = kRequestMode;
        } else {
            mode = kBatchRequestMode;
        }
        int ret = DoRunEngine(sql_case, options, mode);
        if (ret != ENGINE_TEST_RET_SUCCESS) {
            return ret;
        }
    }
    return ENGINE_TEST_RET_SUCCESS;
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, false);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    if (FLAGS_yaml_path != "") {
        return ::fesql::vm::RunSingle(FLAGS_yaml_path);
    } else {
        LOG(WARNING) << "No --yaml_path specified";
        return -1;
    }
}
