/*
 * fesql.cc
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

#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <sched.h>
#include <unistd.h>
#include <iostream>
#include <sstream>

#include "dbms/dbms_server_impl.h"
#include "glog/logging.h"
#include "brpc/server.h"
#include "version.h"

DECLARE_string(endpoint);
DECLARE_int32(port);
DECLARE_int32(thread_pool_size);

DEFINE_string(role, "tablet | dbms | client ", "Set the fesql role");

void SetupLogging(char* argv[]) {
    google::InitGoogleLogging(argv[0]);
}

void StartDBMS(char* argv[]) {
    SetupLogging(argv);
    ::fesql::dbms::DBMSServerImpl* dbms = new ::fesql::dbms::DBMSServerImpl();
    brpc::ServerOptions options;
    options.num_threads = FLAGS_thread_pool_size;
    brpc::Server server;

    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) <<  "Fail to add dbms service";
        exit(1);
    }

    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(WARNING) << "Fail to start dbms server";
        exit(1);
    }

    std::ostringstream oss;
    oss << FESQL_VERSION_MAJOR << "." << FESQL_VERSION_MEDIUM << "." << FESQL_VERSION_MINOR << "." << FESQL_VERSION_BUG;
    LOG(INFO) <<  "start dbms on port " << FLAGS_port << " with version " << oss.str();
    server.set_version(oss.str());
    server.RunUntilAskedToQuit();
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
	if (FLAGS_role == "dbms") {
        StartDBMS(argv);
    } else {
        std::cout << "Start failed! FLAGS_role must be tablet, client, dbms" << std::endl;
        return 1;
    }
    return 0;
}
