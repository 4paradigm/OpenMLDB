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
#include <iostream>
#include <sstream>

#include "dbms/dbms_server_impl.h"
#include "sdk/dbms_sdk.h"
#include "glog/logging.h"
#include "brpc/server.h"
#include "base/linenoise.h"
#include "base/strings.h"
#include "cmd/version.h"

DECLARE_string(endpoint);
DECLARE_int32(port);
DECLARE_int32(thread_pool_size);

DEFINE_string(role, "tablet | dbms | client ", "Set the fesql role");

static ::fesql::sdk::DBMSSdk* dbms_sdk = NULL;

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
    oss << FESQL_VERSION_MAJOR << "." << FESQL_VERSION_MEDIUM
        << "." << FESQL_VERSION_MINOR << "." << FESQL_VERSION_BUG;
    LOG(INFO) <<  "start dbms on port " << FLAGS_port
        << " with version " << oss.str();
    server.set_version(oss.str());
    server.RunUntilAskedToQuit();
}

void HandleCreateGroup(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        std::cout << "invalid input args" << std::endl;
        return;
    }

    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }

    ::fesql::sdk::GroupDef group;
    group.name = args[2];
    ::fesql::sdk::Status status;
    dbms_sdk->CreateGroup(group, status);
    if (status.code == 0) {
        std::cout << "Create group " << args[2] << " success" << std::endl;
    } else {
        std::cout << "Create group failed with error "
            << status.msg << std::endl;
    }
}

void StartClient() {
    std::cout << "Welcome to FeSQL "<< FESQL_VERSION_MAJOR
              << "." << FESQL_VERSION_MEDIUM
              << "." << FESQL_VERSION_MINOR
              << "." << FESQL_VERSION_BUG << std::endl;
    std::string display_prefix =  ">";
    while (true) {
        std::string buf;
        char *line = ::fesql::base::linenoise(display_prefix.c_str());
        if (line == NULL) {
            return;
        }
        if (line[0] != '\0' && line[0] != '/') {
            buf.assign(line);
            if (!buf.empty()) {
                ::fesql::base::linenoiseHistoryAdd(line);
            }
        }
        ::fesql::base::linenoiseFree(line);
        if (buf.empty()) {
            continue;
        }
        std::vector<std::string> parts;
        ::fesql::base::SplitString(buf, " ", parts);
        if (parts[0] == "create") {
            if (parts[1] == "group") {
                HandleCreateGroup(parts);
            }
        }
    }
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_role == "dbms") {
        StartDBMS(argv);
    } else if (FLAGS_role == "client") {
        StartClient();
    } else {
        std::cout << "Start failed! FLAGS_role must be tablet, client, dbms"
            << std::endl;
        return 1;
    }
    return 0;
}
