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

namespace fesql {
namespace cmd {

static ::fesql::sdk::DBMSSdk *dbms_sdk = NULL;
static std::unique_ptr<::fesql::sdk::TabletSdk> table_sdk;
static ::fesql::sdk::DatabaseDef cmd_client_db;

void HandleSQLScript(
    const std::string &script,
    fesql::sdk::Status &status);  // NOLINT (runtime/references)

void HandleEnterDatabase(const std::string &db_name);
void HandleCmd(const fesql::node::CmdNode *cmd_node,
               fesql::sdk::Status &status);  // NOLINT (runtime/references)
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
    DLOG(INFO) << "start tablet on port " << FLAGS_port << " with version "
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
    DLOG(INFO) << "start dbms on port " << FLAGS_port << " with version "
              << oss.str();
    server.set_version(oss.str());
    server.RunUntilAskedToQuit();
}

void StartClient(char *argv[]) {
    SetupLogging(argv);
    std::cout << "Welcome to FeSQL " << FESQL_VERSION_MAJOR << "."
              << FESQL_VERSION_MEDIUM << "." << FESQL_VERSION_MINOR << "."
              << FESQL_VERSION_BUG << std::endl;
    cmd_client_db.name = "";
    std::string log = "fesql";
    std::string display_prefix = ">";
    std::string continue_prefix = "...";
    std::string cmd_str;
    bool cmd_mode = true;
    while (true) {
        std::string buf;
        std::string prefix = "";
        if (cmd_client_db.name.empty()) {
            prefix = log + display_prefix;
        } else {
            prefix = log + "/" + cmd_client_db.name + display_prefix;
        }
        char *line = ::fesql::base::linenoise(
            cmd_mode ? prefix.c_str() : continue_prefix.c_str());
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
            ::fesql::sdk::Status status;
            HandleSQLScript(cmd_str, status);
            if (0 != status.code) {
                std::cout << "ERROR " << status.code << ":" << status.msg
                          << std::endl;
            }
            cmd_str.clear();
            cmd_mode = true;
        } else {
            cmd_str.append("\n");
            cmd_mode = false;
        }
    }
}

void PrintResultSet(std::ostream &stream, ::fesql::sdk::ResultSet *result_set) {
    if (nullptr == result_set || result_set->GetRowCnt() == 0) {
        stream << "Empty set" << std::endl;
        return;
    }
    ::fesql::base::TextTable t('-', '|', '+');

    // Add Header
    for (unsigned i = 0; i < result_set->GetColumnCnt(); i++) {
        t.add(result_set->GetColumnName(i));
    }
    t.endOfRow();

    std::unique_ptr<fesql::sdk::ResultSetIterator> it = result_set->Iterator();
    while (it->HasNext()) {
        it->Next();
        for (unsigned i = 0; i < result_set->GetColumnCnt(); i++) {
            sdk::DataType data_type = result_set->GetColumnType(i);
            switch (data_type) {
                case fesql::sdk::kTypeInt16: {
                    int16_t value = 0;
                    it->GetInt16(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case fesql::sdk::kTypeInt32: {
                    int32_t value = 0;
                    it->GetInt32(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case fesql::sdk::kTypeInt64: {
                    int64_t value = 0;
                    it->GetInt64(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case fesql::sdk::kTypeFloat: {
                    float value = 0;
                    it->GetFloat(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case fesql::sdk::kTypeDouble: {
                    double value = 0;
                    it->GetDouble(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case fesql::sdk::kTypeString: {
                    char *data = NULL;
                    uint32_t size = 0;
                    it->GetString(i, &data, &size);
                    t.add(std::string(data, size));
                    break;
                }
                default: {
                    t.add("NA");
                }
            }
        }
        t.endOfRow();
    }
    stream << t << std::endl;
    if (result_set->GetRowCnt() > 1) {
        stream << result_set->GetRowCnt() << " rows in set" << std::endl;
    } else {
        stream << result_set->GetRowCnt() << " row in set" << std::endl;
    }
}
void PrintTableSchema(std::ostream &stream, const fesql::sdk::Schema *schema) {
    if (nullptr == schema || schema->GetColumnCnt() == 0) {
        stream << "Empty set" << std::endl;
        return;
    }

    uint32_t items_size = schema->GetColumnCnt();

    ::fesql::base::TextTable t('-', '|', '+');
    t.add("Field");
    t.add("Type");
    t.add("Null");
    t.endOfRow();

    for (uint32_t i = 0; i < items_size; i++) {
        t.add(schema->GetColumnName(i));
        t.add(fesql::sdk::DataTypeName(schema->GetColumnType(i)));
        t.add(schema->IsColumnNotNull(i) ? "YES" : "NO");
        t.endOfRow();
    }

    stream << t;
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
    auto items_size = items.size();
    if (items_size > 1) {
        stream << items_size << " rows in set" << std::endl;
    } else {
        stream << items_size << " row in set" << std::endl;
    }
}
void HandleSQLScript(
    const std::string &script,
    fesql::sdk::Status &status) {  // NOLINT (runtime/references)
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            status.code = fesql::common::kRpcError;
            status.msg = "Fail to connect to dbms";
            return;
        }
    }

    {
        fesql::node::NodeManager node_manager;
        fesql::parser::FeSQLParser parser;
        fesql::base::Status sql_status;

        // TODO(chenjing): init with db
        fesql::node::NodePointVector parser_trees;
        parser.parse(script, parser_trees, &node_manager, sql_status);
        if (0 != sql_status.code) {
            status.code = sql_status.code;
            status.msg = sql_status.msg;
            LOG(WARNING) << status.msg;
            return;
        }

        fesql::node::SQLNode *node = parser_trees[0];

        if (nullptr == node) {
            status.msg = "fail to execute cmd: parser tree is null";
            status.code = fesql::common::kPlanError;
            LOG(WARNING) << status.msg;
            return;
        }

        switch (node->GetType()) {
            case fesql::node::kCmdStmt: {
                fesql::node::CmdNode *cmd =
                    dynamic_cast<fesql::node::CmdNode *>(node);
                HandleCmd(cmd, status);
                return;
            }
            case fesql::node::kCreateStmt: {
                fesql::sdk::ExecuteRequst request;
                ::fesql::sdk::ExecuteResult result;
                request.database.name = cmd_client_db.name;
                request.sql = script;
                dbms_sdk->ExecuteScript(request, result, status);
                return;
            }
            case fesql::node::kInsertStmt: {
                if (!table_sdk) {
                    table_sdk =
                        ::fesql::sdk::CreateTabletSdk(FLAGS_tablet_endpoint);
                }

                if (!table_sdk) {
                    status.code = fesql::common::kConnError;
                    status.msg = " Fail to create tablet sdk";
                    return;
                }

                table_sdk->SyncInsert(cmd_client_db.name, script, status);

                if (0 != status.code) {
                    return;
                }
                std::cout << "Insert success" << std::endl;
                return;
            }
            case fesql::node::kExplainSmt: {
                fesql::plan::SimplePlanner planner(&node_manager);
                fesql::node::PlanNodeList plan_trees;
                if (!planner.CreatePlanTree(parser_trees, plan_trees, sql_status)) {
                    return;
                }
                std::cout << "Logical plan: \n" << plan_trees[0];
                return;
            }
            case fesql::node::kFnList:
            case fesql::node::kQuery: {
                if (!table_sdk) {
                    table_sdk =
                        ::fesql::sdk::CreateTabletSdk(FLAGS_tablet_endpoint);
                }

                if (!table_sdk) {
                    status.code = fesql::common::kConnError;
                    status.msg = " Fail to create tablet sdk";
                    return;
                }
                ::fesql::sdk::Query query;
                query.db = cmd_client_db.name;
                query.sql = script;
                std::unique_ptr<::fesql::sdk::ResultSet> rs =
                    table_sdk->SyncQuery(query, status);
                if (!rs) {
                    std::cout << "Fail to query sql: " << status.msg
                              << std::endl;
                } else {
                    PrintResultSet(std::cout, rs.get());
                }
                return;
            }
            default: {
                status.msg = "Fail to execute script with unSuppurt type" +
                             fesql::node::NameOfSQLNodeType(node->GetType());
                status.code = fesql::common::kUnSupport;
                return;
            }
        }
    }
}
void HandleCmd(const fesql::node::CmdNode *cmd_node,
               fesql::sdk::Status &status) {  // NOLINT (runtime/references)
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    ::fesql::sdk::DatabaseDef db;
    db.name = cmd_client_db.name;
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
                PrintItems(std::cout, "Tables_In_" + cmd_client_db.name, names);
            }
            return;
        }
        case fesql::node::kCmdDescTable: {
            fesql::type::TableDef table;
            std::unique_ptr<::fesql::sdk::Schema> rs =
                dbms_sdk->GetSchema(db, cmd_node->GetArgs()[0], status);
            if (!rs) {
                return;
            } else {
                PrintTableSchema(std::cout, rs.get());
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
        case fesql::node::kCmdSource: {
            std::ifstream in;
            in.open(cmd_node->GetArgs()[0]);  // open the input file
            if (!in.is_open()) {
                status.code = fesql::common::kFileIOError;
                status.msg = "Incorrect file path";
                return;
            }
            std::stringstream str_stream;
            str_stream << in.rdbuf();  // read the file
            std::string str =
                str_stream.str();  // str holds the content of the file
            ::fesql::sdk::ExecuteRequst requst;
            ::fesql::sdk::ExecuteResult result;

            requst.database.name = cmd_client_db.name;
            requst.sql = str;
            dbms_sdk->ExecuteScript(requst, result, status);
            if (0 == status.code) {
                std::cout << "Create table success" << std::endl;
                return;
            }
            break;
        }
        case fesql::node::kCmdUseDatabase: {
            fesql::sdk::DatabaseDef usedb;
            usedb.name = cmd_node->GetArgs()[0];
            if (0 != status.code) {
                return;
            }
            if (dbms_sdk->IsExistDatabase(usedb, status)) {
                cmd_client_db.name = usedb.name;
                std::cout << "Database changed" << std::endl;
            } else {
                std::cout << "Database '" << usedb.name << "' not exists"
                          << std::endl;
            }
            break;
        }
        case fesql::node::kCmdExit: {
            exit(0);
        }
        default: {
            status.code = fesql::common::kUnSupport;
            status.msg = "UnSupport Cmd " +
                         fesql::node::CmdTypeName(cmd_node->GetCmdType());
        }
    }
}

}  // namespace cmd
}  // namespace fesql

int main(int argc, char *argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_role == "dbms") {
        ::fesql::cmd::StartDBMS(argv);
    } else if (FLAGS_role == "tablet") {
        ::fesql::cmd::StartTablet(argc, argv);
    } else if (FLAGS_role == "client") {
        ::fesql::cmd::StartClient(argv);
    } else if (FLAGS_role == "csv") {
    } else {
        std::cout << "Start failed! FLAGS_role must be tablet, client, dbms"
                  << std::endl;
        return 1;
    }
    return 0;
}
