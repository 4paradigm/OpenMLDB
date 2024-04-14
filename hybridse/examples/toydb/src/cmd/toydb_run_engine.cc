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

#include "testing/toydb_engine_test_base.h"

#include "gflags/gflags.h"

DEFINE_string(yaml_path, "", "Yaml filepath to load cases from");
DEFINE_string(runner_mode, "batch",
              "Specify runner mode, can be batch or request");
DEFINE_string(cluster_mode, "standalone",
              "Specify cluster mode, can be standalone or cluster");
DEFINE_bool(
    enable_batch_request_opt, true,
    "Specify whether perform batch request optimization in batch request mode");
DEFINE_bool(enable_expr_opt, true,
            "Specify whether do expression optimization");
DEFINE_int32(run_iters, 0, "Measure the approximate run time if specified");
DEFINE_string(case_id, "", "Specify the case id to run and skip others");

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
    // TODO(ace): it become impossible for those want to give case_dir as absolute path, because final case dir always
    // prefixed with base dir. Should support both
    if (!SqlCase::CreateSqlCasesFromYaml(SqlCase::SqlCaseBaseDir(), yaml_path, cases)) {
        LOG(WARNING) << "Load cases from " << yaml_path << " failed";
        return ENGINE_TEST_RET_INVALID_CASE;
    }
    EngineOptions options;
    options.SetClusterOptimized(absl::EqualsIgnoreCase(FLAGS_cluster_mode, "cluster"));
    options.SetBatchRequestOptimized(FLAGS_enable_batch_request_opt);
    options.SetEnableExprOptimize(FLAGS_enable_expr_opt);
    JitOptions& jit_options = options.jit_options();
    jit_options.SetEnableMcjit(FLAGS_enable_mcjit);
    jit_options.SetEnableVtune(FLAGS_enable_vtune);
    jit_options.SetEnableGdb(FLAGS_enable_gdb);
    jit_options.SetEnablePerf(FLAGS_enable_perf);

    for (auto& sql_case : cases) {
        if (!FLAGS_case_id.empty() && FLAGS_case_id != sql_case.id()) {
            continue;
        }
        EngineMode default_mode;
        if (absl::EqualsIgnoreCase(FLAGS_runner_mode, "batch")) {
            default_mode = kBatchMode;
        } else if (absl::EqualsIgnoreCase(FLAGS_runner_mode, "request")) {
            default_mode = kRequestMode;
        } else {
            default_mode = kBatchRequestMode;
        }
        auto mode = Engine::TryDetermineMode(sql_case.sql_str_, default_mode);
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
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    if (FLAGS_yaml_path != "") {
        return ::hybridse::vm::RunSingle(FLAGS_yaml_path);
    } else {
        LOG(WARNING) << "No --yaml_path specified";
        return -1;
    }
}
