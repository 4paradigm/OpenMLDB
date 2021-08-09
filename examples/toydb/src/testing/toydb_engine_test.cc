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

#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "testing/toydb_engine_test_base.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace hybridse {
namespace vm {
TEST_P(EngineTest, test_request_engine) {
    ParamType sql_case = GetParam();
    EngineOptions options;
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "request-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-unsupport")) {
        EngineCheck(sql_case, options, kRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_P(EngineTest, test_batch_engine) {
    ParamType sql_case = GetParam();
    EngineOptions options;
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "batch-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-batch-unsupport")) {
        EngineCheck(sql_case, options, kBatchMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_P(EngineTest, test_batch_request_engine_for_last_row) {
    ParamType sql_case = GetParam();
    EngineOptions options;
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "request-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "batch-request-unsupport")) {
        EngineCheck(sql_case, options, kBatchRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_P(EngineTest, test_cluster_request_engine) {
    ParamType sql_case = GetParam();
    EngineOptions options;
    options.set_cluster_optimized(true);
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "request-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "cluster-unsupport")) {
        EngineCheck(sql_case, options, kRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_P(EngineTest, test_cluster_batch_request_engine) {
    ParamType sql_case = GetParam();
    EngineOptions options;
    options.set_cluster_optimized(true);
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "request-unsupport") &&
        !boost::contains(sql_case.mode(), "rtidb-unsupport") &&
        !boost::contains(sql_case.mode(), "batch-request-unsupport") &&
        !boost::contains(sql_case.mode(), "cluster-unsupport")) {
        EngineCheck(sql_case, options, kBatchRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}

TEST_P(BatchRequestEngineTest, test_batch_request_engine) {
    ParamType sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    EngineOptions options;
    options.set_cluster_optimized(false);
    if (!boost::contains(sql_case.mode(), "batch-request-unsupport")) {
        EngineCheck(sql_case, options, kBatchRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}
TEST_P(BatchRequestEngineTest, test_cluster_batch_request_engine) {
    ParamType sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    EngineOptions options;
    options.set_cluster_optimized(true);
    if (!boost::contains(sql_case.mode(), "batch-request-unsupport") &&
        !boost::contains(sql_case.mode(), "cluster-unsupport")) {
        EngineCheck(sql_case, options, kBatchRequestMode);
    } else {
        LOG(INFO) << "Skip mode " << sql_case.mode();
    }
}

}  // namespace vm
}  // namespace hybridse

int main(int argc, char** argv) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::testing::InitGoogleTest(&argc, argv);
    // ::hybridse::vm::CoreAPI::EnableSignalTraceback();
    return RUN_ALL_TESTS();
}
