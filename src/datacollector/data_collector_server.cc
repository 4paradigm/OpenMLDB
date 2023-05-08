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

#include "datacollector/data_collector.h"

#include "brpc/server.h"
#include "gflags/gflags.h"

#include "base/glog_wrapper.h"
#include "version.h"  // NOLINT

DECLARE_string(endpoint);
DECLARE_int32(thread_pool_size);

const std::string OPENMLDB_VERSION = std::to_string(OPENMLDB_VERSION_MAJOR) + "." +  // NOLINT
                                     std::to_string(OPENMLDB_VERSION_MINOR) + "." +
                                     std::to_string(OPENMLDB_VERSION_BUG) + "-" + OPENMLDB_COMMIT_ID;

int main(int argc, char* argv[]) {
    ::google::SetVersionString(OPENMLDB_VERSION);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_role = "data_collector";
    openmldb::base::SetupGlog();
    // no logic about real_endpoint
    auto data_collector = new openmldb::datacollector::DataCollectorImpl();
    if (!data_collector->Init(FLAGS_endpoint)) {
        LOG(WARNING) << "init data collector failed";
        exit(1);
    }
    if (!data_collector->RegisterZK()) {
        LOG(WARNING) << "Fail to register zk";
        exit(1);
    }

    brpc::ServerOptions options;
    options.num_threads = FLAGS_thread_pool_size;
    brpc::Server server;
    if (server.AddService(data_collector, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add service";
        exit(1);
    }

    if (server.Start(FLAGS_endpoint.c_str(), &options) != 0) {
        LOG(WARNING) << "Fail to start server";
        exit(1);
    }
    LOG(INFO) << "start data collector on endpoint " << FLAGS_endpoint << " with version " << OPENMLDB_VERSION;
    server.set_version(OPENMLDB_VERSION.c_str());
    server.RunUntilAskedToQuit();
    return 0;
}
