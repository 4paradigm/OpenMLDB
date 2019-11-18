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
#include <string>
#include "analyser/analyser.h"
#include "base/texttable.h"
#include "plan/planner.h"
#include "sdk/tablet_sdk.h"

#include "base/linenoise.h"
#include "base/strings.h"
#include "brpc/server.h"
#include "cmd/version.h"
#include "dbms/dbms_server_impl.h"
#include "glog/logging.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "sdk/dbms_sdk.h"
#include "tablet/tablet_server_impl.h"

DECLARE_string(endpoint);
DECLARE_string(tablet_endpoint);
DECLARE_int32(port);
DECLARE_int32(thread_pool_size);

DEFINE_string(role, "tablet | dbms | client ", "Set the fesql role");

static ::fesql::sdk::DBMSSdk *dbms_sdk = NULL;
static std::unique_ptr<::fesql::sdk::TabletSdk> table_sdk;
static ::std::string cmd_client_db = "";

void HandleSQLScript(std::string basicString, fesql::base::Status &status);
void HandleCreateSchema(std::string str, ::fesql::base::Status &status);

void HandleEnterDatabase(std::string &db_name);
void handleCmd(fesql::node::CmdNode *cmd_node, fesql::base::Status &status);
void SetupLogging(char *argv[]) { google::InitGoogleLogging(argv[0]); }

void StartTablet(int argc, char *argv[]) {
    SetupLogging(argv);
    ::llvm::InitLLVM X(argc, argv);
    ::llvm::InitializeNativeTarget();
    ::llvm::InitializeNativeTargetAsmPrinter();
    ::fesql::tablet::TabletServerImpl *tablet =
        new ::fesql::tablet::TabletServerImpl();
    bool ok = tablet->Init();
    if (!ok) {
        LOG(WARNING) << "Fail to init tablet service";
        exit(1);
    }

    brpc::ServerOptions options;
    options.num_threads = FLAGS_thread_pool_size;
    brpc::Server server;

    if (server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add tablet service";
        exit(1);
    }

    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(WARNING) << "Fail to start tablet server";
        exit(1);
    }

    std::ostringstream oss;
    oss << FESQL_VERSION_MAJOR << "." << FESQL_VERSION_MEDIUM << "."
        << FESQL_VERSION_MINOR << "." << FESQL_VERSION_BUG;
    LOG(INFO) << "start tablet on port " << FLAGS_port << " with version "
              << oss.str();
    server.set_version(oss.str());
    server.RunUntilAskedToQuit();
}

void StartDBMS(char *argv[]) {
    SetupLogging(argv);
    ::fesql::dbms::DBMSServerImpl *dbms = new ::fesql::dbms::DBMSServerImpl();
    dbms->SetTabletEndpoint(FLAGS_tablet_endpoint);
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
            ::fesql::base::Status status;
            HandleSQLScript(cmd_str, status);
            if (0 != status.code) {
                std::cout << "ERROR " << status.code << ":" << status.msg
                          << std::endl;
            }
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

void PrintResultSet(std::ostream &stream,
                    const ::fesql::sdk::ResultSet *result_set) {
    if (nullptr == result_set || result_set->GetColumnCnt() == 0) {
        stream << "Empty set" << std::endl;
        return;
    }
    ::fesql::base::TextTable t('-', '|', '+');

    for (unsigned i = 0; i < result_set->GetColumnCnt(); i++) {
        t.add(result_set->GetColumnName(i));
    }
    t.endOfRow();
    stream << t << std::endl;
}
void PrintTableSchema(std::ostream &stream,
                      const fesql::type::TableDef &table) {
    ::fesql::base::TextTable t('-', '|', '+');

    t.add("Field");
    t.add("Type");
    t.add("Null");
    t.endOfRow();

    for (auto column : table.columns()) {
        t.add(column.name());
        t.add(fesql::type::Type_Name(column.type()));
        t.add(column.is_null() ? "YES" : "NO");
        t.endOfRow();
    }
    stream << t;
    unsigned items_size = table.columns().size();
    if (items_size > 1) {
        stream << items_size << " rows in set" << std::endl;
    } else {
        stream << items_size << " row in set" << std::endl;
    }
}

void PrintItems(std::ostream &stream, const std::string &head,
                const std::vector<std::string> &items) {
    if (items.empty()) {
        stream << "Empty set" << std::endl;
        return;
    }

    ::fesql::base::TextTable t('-', '|', '+');
    t.add(head);
    t.endOfRow();
    for (auto item : items) {
        t.add(item);
        t.endOfRow();
    }
    stream << t;
    unsigned items_size = items.size();
    if (items_size > 1) {
        stream << items_size << " rows in set" << std::endl;
    } else {
        stream << items_size << " row in set" << std::endl;
    }
}
void HandleSQLScript(std::string script, fesql::base::Status &status) {
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            status.code = fesql::error::kRpcErrorConnection;
            status.msg = "Fail to connect to dbms";
            return;
        }
    }

    {
        fesql::node::NodeManager node_manager;
        fesql::parser::FeSQLParser parser;
        fesql::analyser::FeSQLAnalyser analyser(&node_manager);
        fesql::plan::SimplePlanner planner(&node_manager);

        // TODO(chenjing): init with db
        fesql::node::NodePointVector parser_trees;
        parser.parse(script, parser_trees, &node_manager, status);
        if (0 != status.code) {
            LOG(WARNING) << status.msg;
            return;
        }

        fesql::node::SQLNode *node = parser_trees[0];

        if (nullptr == node) {
            status.msg = "fail to execute cmd: parser tree is null";
            status.code = fesql::error::kCmdErrorNullNode;
            LOG(WARNING) << status.msg;
            return;
        }
        switch (node->GetType()) {
            case fesql::node::kCmdStmt: {
                fesql::node::CmdNode *cmd =
                    dynamic_cast<fesql::node::CmdNode *>(node);
                handleCmd(cmd, status);
                return;
            }
            case fesql::node::kCreateStmt: {
                fesql::sdk::ExecuteRequst request;
                ::fesql::sdk::ExecuteResult result;
                request.database.name = cmd_client_db;
                request.sql = script;
                dbms_sdk->ExecuteScript(request, result, status);
                return;
            }
            case fesql::node::kSelectStmt: {
                if (!table_sdk) {
                    table_sdk =
                        ::fesql::sdk::CreateTabletSdk(FLAGS_tablet_endpoint);
                }

                if (!table_sdk) {
                    status.code = fesql::error::kCmdErrorNullNode;
                    status.msg = " Fail to create tablet sdk";
                    return;
                }
                ::fesql::sdk::Query query;
                query.db = cmd_client_db;
                query.sql = script;
                std::unique_ptr<::fesql::sdk::ResultSet> rs =
                    table_sdk->SyncQuery(query);
                if (!rs) {
                    std::cout << "Fail to query sql: " << status.msg << std::endl;
                } else {
                    PrintResultSet(std::cout, rs.get());
                }
                return;
            }
            default: {
                status.msg = "Fail to execute script with unSuppurt type" +
                             fesql::node::NameOfSQLNodeType(node->GetType());
                status.code = fesql::error::kExecuteErrorUnSupport;
                return;
            }
        }
    }
}
void handleCmd(fesql::node::CmdNode *cmd_node, fesql::base::Status &status) {
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    ::fesql::sdk::DatabaseDef db;
    db.name = cmd_client_db;
    switch (cmd_node->GetCmdType()) {
        case fesql::node::kCmdShowDatabases: {
            std::vector<std::string> names;
            dbms_sdk->GetDatabases(names, status);
            if (status.code == 0) {
                PrintItems(std::cout, "Databases", names);
            }
            return;
        }
        case fesql::node::kCmdShowTables: {
            std::vector<std::string> names;
            dbms_sdk->GetTables(db, names, status);
            if (status.code == 0) {
                std::ostringstream oss;
                PrintItems(std::cout, "Tables_In_" + cmd_client_db, names);
            }
            return;
        }
        case fesql::node::kCmdDescTable: {
            fesql::type::TableDef table;
            dbms_sdk->GetSchema(db, cmd_node->GetArgs()[0], table, status);
            if (status.code == 0) {
                std::ostringstream oss;
                PrintTableSchema(std::cout, table);
            }
            break;
        }
        case fesql::node::kCmdCreateGroup: {
            fesql::sdk::GroupDef group;
            group.name = cmd_node->GetArgs()[0];
            dbms_sdk->CreateGroup(group, status);
            if (0 == status.code) {
                std::cout << "Create group success" << std::endl;
            }
            break;
        }
        case fesql::node::kCmdCreateDatabase: {
            fesql::sdk::DatabaseDef new_db;
            new_db.name = cmd_node->GetArgs()[0];
            dbms_sdk->CreateDatabase(new_db, status);
            if (0 == status.code) {
                std::cout << "Create database success" << std::endl;
            }
            break;
        }
        case fesql::node::kCmdCreateTable: {
            std::ifstream in;
            in.open(cmd_node->GetArgs()[0]);  // open the input file
            if (!in.is_open()) {
                status.code = fesql::error::kCmdErrorPathError;
                status.msg = "Incorrect file path";
                return;
            }
            std::stringstream str_stream;
            str_stream << in.rdbuf();  // read the file
            std::string str =
                str_stream.str();  // str holds the content of the file
            ::fesql::base::Status status;
            ::fesql::sdk::ExecuteRequst requst;
            ::fesql::sdk::ExecuteResult result;

            requst.database.name = cmd_client_db;
            requst.sql = str;
            dbms_sdk->ExecuteScript(requst, result, status);
            if (0 == status.code) {
                std::cout << "Create table success" << std::endl;
            }
            break;
        }
        case fesql::node::kCmdUseDatabase: {
            fesql::sdk::DatabaseDef usedb;
            usedb.name = cmd_node->GetArgs()[0];
            if (0 == status.code) {
                if (dbms_sdk->IsExistDatabase(usedb, status)) {
                    cmd_client_db = usedb.name;
                    std::cout << "Database changed" << std::endl;
                } else {
                    std::cout << "Database '" << usedb.name << "' not exists"
                              << std::endl;
                }
            }

            break;
        }
        default: {
            status.code = fesql::error::kCmdErrorUnSupport;
            status.msg = "UnSupport Cmd " +
                         fesql::node::CmdTypeName(cmd_node->GetCmdType());
        }
    }
}

int main(int argc, char *argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_role == "dbms") {
        StartDBMS(argv);
    } else if (FLAGS_role == "tablet") {
        StartTablet(argc, argv);
    } else if (FLAGS_role == "client") {
        StartClient(argv);
    } else {
        std::cout << "Start failed! FLAGS_role must be tablet, client, dbms"
                  << std::endl;
        return 1;
    }
    return 0;
}
