/*
 * Copyright 2023 4paradigm authors
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

// Run the given YAML cases under SQLClusterRouter connection
//
// The cases by default run once, with expect result assertion.
// Or run repeated times as a tiny benchmark.

#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gflags/gflags.h"
#include "sdk/sql_router.h"
#include "sdk/sql_sdk_base_test.h"

DEFINE_string(yaml_path, "", "Yaml filepath to load cases from");
DEFINE_string(case_id, "", "case id");

DEFINE_string(zk, "", "endpoint to zookeeper");
DEFINE_string(zk_path, "/openmldb", "zookeeper root path for openmldb cluster");

DEFINE_bool(keep_data, false,
            R"s(keep the data during case preparation, e.g tables, procedures, deployments after test ends.
            Be careful turning this on when running the same case multiple times)s");

DEFINE_uint32(repeat, 1, "repeat times for single case");
DEFINE_uint32(repeat_interval, 0, "set random interval between repeating runs, 0 means no wait, unit: milliseconds");
DEFINE_bool(skip_prepare, false, "skip database, table & deployment create, take your own risk");
DEFINE_bool(skip_data, false, "skip insert rows to tables during data preparation, take your own risk");
DEFINE_bool(query_only, false, "if true, request row won't inserted into table after deployment/procedure query");
DEFINE_uint32(threads, 1, "thread number for deployment query");
DEFINE_bool(extreme, false, "calls deploy only, no result check, no inserts, no logs, simply request & return");

namespace openmldb {
namespace sdk {

static absl::BitGen gen;

int Run(std::shared_ptr<SQLRouter> router, absl::string_view yaml_path, bool cleanup) {
    std::vector<::hybridse::sqlcase::SqlCase> cases;
    if (!::hybridse::sqlcase::SqlCase::CreateSqlCasesFromYaml(hybridse::sqlcase::SqlCase::SqlCaseBaseDir(),
                                                              std::string(yaml_path), cases)) {
        LOG(WARNING) << "Load cases from " << yaml_path << " failed";
        return 1;
    }

    for (auto& sql_case : cases) {
        if (!FLAGS_case_id.empty() && FLAGS_case_id != sql_case.id()) {
            continue;
        }

        DeploymentEnv env(router, &sql_case);
        env.SetCleanup(cleanup);
        env.SetPureDeploy(FLAGS_query_only);
        if (!FLAGS_skip_prepare) {
            env.SetUp(FLAGS_skip_data);
        }

        absl::Duration dur = absl::Milliseconds(0);
        uint64_t runs_cnt = 0;
        auto call_once = [&dur, &env, &runs_cnt](absl::Mutex* mu) {
            while (true) {
                if (FLAGS_repeat_interval > 0) {
                    absl::Duration random_interval = absl::Milliseconds(absl::Uniform(gen, 1u, FLAGS_repeat_interval));
                    LOG(INFO) << "sleep for " << random_interval;
                    absl::SleepFor(random_interval);
                }
                absl::Time start = absl::Now();

                if (FLAGS_extreme) {
                    env.CallDeployProcedureTiny();
                } else {
                    env.CallDeployProcedure();
                }

                absl::Time end = absl::Now();

                absl::MutexLock lock(mu);
                dur += end - start;
                runs_cnt++;
                if (runs_cnt >= FLAGS_repeat) {
                    return;
                }
            }
        };

        if (FLAGS_threads > 1) {
            absl::Mutex mutex;
            std::vector<std::thread> threads;
            threads.reserve(FLAGS_threads);
            for (decltype(FLAGS_threads) i = 0; i < FLAGS_threads; ++i) {
                threads.emplace_back(call_once, &mutex);
            }
            for (auto& t : threads) {
                t.join();
            }
        } else {
            // seq call
            for (decltype(FLAGS_repeat) i = 0; i < FLAGS_repeat; ++i) {
                if (FLAGS_repeat_interval > 0) {
                    absl::Duration random_interval = absl::Milliseconds(absl::Uniform(gen, 1u, FLAGS_repeat_interval));
                    LOG(INFO) << "sleep for " << random_interval;
                    absl::SleepFor(random_interval);
                }
                absl::Time start = absl::Now();

                if (FLAGS_extreme) {
                    env.CallDeployProcedureTiny();
                } else {
                    env.CallDeployProcedure();
                }

                dur += absl::Now() - start;
            }
        }

        LOG(INFO) << "Case " << sql_case.id_ << " " << sql_case.desc_ << " costs " << dur << " for " << FLAGS_repeat
                  << " runs. Avg " << dur / FLAGS_repeat;
    }

    return 0;
}

}  // namespace sdk
}  // namespace openmldb

int main(int argc, char *argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, false);

    openmldb::sdk::SQLRouterOptions opts;
    opts.zk_cluster = FLAGS_zk;
    opts.zk_path = FLAGS_zk_path;
    opts.enable_debug = ::hybridse::sqlcase::SqlCase::IsDebug();

    auto router = openmldb::sdk::NewClusterSQLRouter(opts);
    if (router == nullptr) {
        LOG(ERROR) << "Fail to init OpenMLDB connection";
        return 1;
    }

    hybridse::sdk::Status s;
    router->ExecuteSQL("set @@execute_mode = 'online'", &s);

    return ::openmldb::sdk::Run(router, FLAGS_yaml_path, !FLAGS_keep_data);
}
