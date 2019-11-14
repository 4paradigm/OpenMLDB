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

#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "base/linenoise.h"
#include "base/strings.h"
#include "brpc/server.h"
#include "cmd/version.h"
#include "dbms/dbms_server_impl.h"
#include "glog/logging.h"
#include "sdk/dbms_sdk.h"

DECLARE_string(endpoint);
DECLARE_string(db);
DECLARE_int32(port);
DECLARE_int32(thread_pool_size);

DEFINE_string(role, "tablet | dbms | client ", "Set the fesql role");

static ::fesql::sdk::DBMSSdk *dbms_sdk = NULL;
static ::std::string cmd_client_db = "";

void HandleSQLScript(std::string basicString);
void HandleCreateSchema(std::string str, ::fesql::base::Status &status);

void HandleEnterDatabase(std::string &db_name);
void SetupLogging(char *argv[]) { google::InitGoogleLogging(argv[0]); }

void StartDBMS(char *argv[]) {
    SetupLogging(argv);
    ::fesql::dbms::DBMSServerImpl *dbms = new ::fesql::dbms::DBMSServerImpl();
    brpc::ServerOptions options;
    options.num_threads = FLAGS_thread_pool_size;
    brpc::Server server;

    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }

    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(WARNING) << "Fail to start dbms server";
        exit(1);
    }

    std::ostringstream oss;
    oss << FESQL_VERSION_MAJOR << "." << FESQL_VERSION_MEDIUM << "."
        << FESQL_VERSION_MINOR << "." << FESQL_VERSION_BUG;
    LOG(INFO) << "start dbms on port " << FLAGS_port << " with version "
              << oss.str();
    server.set_version(oss.str());
    server.RunUntilAskedToQuit();
}

void StartClient(char *argv[]) {
    SetupLogging(argv);
    std::cout << "Welcome to FeSQL " << FESQL_VERSION_MAJOR << "."
              << FESQL_VERSION_MEDIUM << "." << FESQL_VERSION_MINOR << "."
              << FESQL_VERSION_BUG << std::endl;
    cmd_client_db = "";
    std::string display_prefix = ">";
    std::string continue_prefix = "...";
    std::string cmd_str;
    bool cmd_mode = true;
    while (true) {
        std::string buf;
        char *line = ::fesql::base::linenoise(
            cmd_mode ? (cmd_client_db + display_prefix).c_str()
                     : continue_prefix.c_str());
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

        cmd_str.append(buf);
        // TODO(CHENJING) remove
        if (cmd_str.back() == ';') {
            HandleSQLScript(cmd_str);
            cmd_str.clear();
            cmd_mode = true;
        } else {
            cmd_mode = false;
        }
    }
}
void HandleEnterDatabase(std::string &db_name) {
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    ::fesql::base::Status status;
    ::fesql::sdk::DatabaseDef database;
    database.name = db_name;
    bool exist = dbms_sdk->IsExistDatabase(database, status);
    if (status.code == 0) {
        if (exist) {
            cmd_client_db = db_name;
        } else {
            std::cout << "ERROR : Database " << db_name << " doesn't exist";
        }
    } else {
        std::cout << "ERROR " << status.code << ":" << status.msg << std::endl;
    }
}
void HandleSQLScript(std::string script) {
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    ::fesql::base::Status status;
    fesql::sdk::ExecuteRequst request;
    ::fesql::sdk::ExecuteResult result;
    request.database.name = cmd_client_db;
    request.sql = script;
    dbms_sdk->ExecuteScript(request, result, status);
    if (status.code != 0) {
        std::cout << "ERROR " << status.code << ":" << status.msg << std::endl;
    } else {
        if (!result.database.name.empty()) {
            cmd_client_db = result.database.name;
        }
        if (!result.result.empty()) {
            std::cout << result.result << std::endl;
        }
    }
}

int main(int argc, char *argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_role == "dbms") {
        StartDBMS(argv);
    } else if (FLAGS_role == "client") {
        StartClient(argv);
    } else {
        std::cout << "Start failed! FLAGS_role must be tablet, client, dbms"
                  << std::endl;
        return 1;
    }
    return 0;
}
