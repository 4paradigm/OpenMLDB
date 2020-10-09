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
#include "vm/engine_test.h"

DEFINE_string(yaml_path, "", "Yaml filepath to load cases from");
DEFINE_string(runner_mode, "batch",
              "Specify runner mode, can be batch or request");

namespace fesql {
namespace vm {

int RunSingle(const std::string& yaml_path) {
    std::vector<SQLCase> cases;
    if (!SQLCase::CreateSQLCasesFromYaml("", yaml_path, cases)) {
        LOG(WARNING) << "Load cases from " << yaml_path << " failed";
        return ENGINE_TEST_RET_INVALID_CASE;
    }
    for (auto& sql_case : cases) {
        bool is_batch = FLAGS_runner_mode == "batch";
        EngineMode mode = is_batch ? kBatchMode : kRequestMode;
        bool check_compatible = false;
        int ret;
        EngineCheck(sql_case, mode, check_compatible, &ret);
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
