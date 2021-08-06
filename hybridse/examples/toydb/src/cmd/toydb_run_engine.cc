/*
 * Copyright 2021 4Paradigm
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
#include "testing/toydb_engine_test_base.h"

DEFINE_string(yaml_path, "", "Yaml filepath to load cases from");
DEFINE_string(runner_mode, "batch",
              "Specify runner mode, can be batch or request");
DEFINE_string(cluster_mode, "standalone",
              "Specify cluster mode, can be standalone or cluster");
DEFINE_bool(
    enable_batch_request_opt, true,
    "Specify whether perform batch request optimization in batch request mode");
DEFINE_bool(performance_sensitive, true,
            "Specify whether do performance sensitive check");
DEFINE_bool(enable_expr_opt, true,
            "Specify whether do expression optimization");
DEFINE_bool(
    enable_batch_window_parallelization, false,
    "Specify whether enable window parallelization in spark batch mode");
DEFINE_int32(run_iters, 0, "Measure the approximate run time if specified");
DEFINE_int32(case_id, -1, "Specify the case id to run and skip others");

// jit options
DEFINE_bool(enable_mcjit, false, "Use llvm legacy mcjit engine");
DEFINE_bool(enable_vtune, false, "Enable llvm jit vtune events");
DEFINE_bool(enable_gdb, false, "Enable llvm jit gdb events");
DEFINE_bool(enable_perf, false, "Enable llvm jit perf events");

namespace hybridse {
namespace vm {

int DoRunEngine(const SqlCase& sql_case, const EngineOptions& options,
                EngineMode engine_mode) {
    std::shared_ptr<EngineTestRunner> runner;
    if (engine_mode == kBatchMode) {
        runner =
            std::make_shared<ToydbBatchEngineTestRunner>(sql_case, options);
    } else if (engine_mode == kRequestMode) {
        runner =
            std::make_shared<ToydbRequestEngineTestRunner>(sql_case, options);
    } else {
        runner = std::make_shared<ToydbBatchRequestEngineTestRunner>(
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
    std::vector<SqlCase> cases;
    if (!SqlCase::CreateSqlCasesFromYaml("", yaml_path, cases)) {
        LOG(WARNING) << "Load cases from " << yaml_path << " failed";
        return ENGINE_TEST_RET_INVALID_CASE;
    }
    EngineOptions options;
    options.set_cluster_optimized(FLAGS_cluster_mode == "cluster");
    options.set_batch_request_optimized(FLAGS_enable_batch_request_opt);
    options.set_performance_sensitive(FLAGS_performance_sensitive);
    options.set_enable_expr_optimize(FLAGS_enable_expr_opt);
    options.set_enable_batch_window_parallelization(
        FLAGS_enable_batch_window_parallelization);

    JitOptions& jit_options = options.jit_options();
    jit_options.set_enable_mcjit(FLAGS_enable_mcjit);
    jit_options.set_enable_vtune(FLAGS_enable_vtune);
    jit_options.set_enable_gdb(FLAGS_enable_gdb);
    jit_options.set_enable_perf(FLAGS_enable_perf);

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
}  // namespace hybridse

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, false);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    if (FLAGS_yaml_path != "") {
        return ::hybridse::vm::RunSingle(FLAGS_yaml_path);
    } else {
        LOG(WARNING) << "No --yaml_path specified";
        return -1;
    }
}
