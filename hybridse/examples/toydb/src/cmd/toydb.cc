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

#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <string>

#include "absl/strings/str_cat.h"
#include "base/fe_linenoise.h"
#include "base/fe_strings.h"
#include "base/texttable.h"
#include "brpc/server.h"
#include "dbms/dbms_server_impl.h"
#include "glog/logging.h"
#include "hybridse_version.h"  //NOLINT
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "plan/plan_api.h"
#include "sdk/dbms_sdk.h"
#include "sdk/tablet_sdk.h"
#include "tablet/tablet_server_impl.h"

DECLARE_string(toydb_endpoint);
DECLARE_string(tablet_endpoint);
DECLARE_int32(toydb_port);
DECLARE_int32(toydb_thread_pool_size);
DEFINE_string(role, "tablet | dbms | client ", "Set the hybridse role");

namespace hybridse {
namespace cmd {

static std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk;
static std::shared_ptr<::hybridse::sdk::TabletSdk> table_sdk;

struct DBContxt {
    std::string name;
};
static DBContxt cmd_client_db;

void HandleSqlScript(const std::string &script,
                     hybridse::sdk::Status &status);  // NOLINT (runtime/references)

void HandleEnterDatabase(const std::string &db_name);
void HandleCmd(const hybridse::node::CmdPlanNode *cmd_node,
               hybridse::sdk::Status &status);  // NOLINT (runtime/references)
void SetupLogging(char *argv[]) { google::InitGoogleLogging(argv[0]); }

void StartTablet(int argc, char *argv[]) {
    ::llvm::InitLLVM X(argc, argv);
    ::llvm::InitializeNativeTarget();
    ::llvm::InitializeNativeTargetAsmPrinter();
    ::hybridse::tablet::TabletServerImpl *tablet = new ::hybridse::tablet::TabletServerImpl();
    bool ok = tablet->Init();
    if (!ok) {
        LOG(WARNING) << "Fail to init tablet service";
        exit(1);
    }

    brpc::ServerOptions options;
    options.num_threads = FLAGS_toydb_thread_pool_size;
    brpc::Server server;

    if (server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add tablet service";
        exit(1);
    }

    if (server.Start(FLAGS_toydb_port, &options) != 0) {
        LOG(WARNING) << "Fail to start tablet server";
        exit(1);
    }

    std::ostringstream oss;
    oss << HYBRIDSE_VERSION_MAJOR << "." << HYBRIDSE_VERSION_MINOR << "." << HYBRIDSE_VERSION_BUG;
    DLOG(INFO) << "start tablet on port " << FLAGS_toydb_port << " with version " << oss.str();
    server.set_version(oss.str());
    server.RunUntilAskedToQuit();
}

void StartDBMS(char *argv[]) {
    SetupLogging(argv);
    ::hybridse::dbms::DBMSServerImpl *dbms = new ::hybridse::dbms::DBMSServerImpl();
    brpc::ServerOptions options;
    options.num_threads = FLAGS_toydb_thread_pool_size;
    brpc::Server server;

    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }

    if (server.Start(FLAGS_toydb_port, &options) != 0) {
        LOG(WARNING) << "Fail to start dbms server";
        exit(1);
    }

    std::ostringstream oss;
    oss << HYBRIDSE_VERSION_MAJOR << "." << HYBRIDSE_VERSION_MINOR << "." << HYBRIDSE_VERSION_BUG;
    DLOG(INFO) << "start dbms on port " << FLAGS_toydb_port << " with version " << oss.str();
    server.set_version(oss.str());
    server.RunUntilAskedToQuit();
}

void StartClient(char *argv[]) {
    SetupLogging(argv);
    std::cout << "Welcome to TOYDB " << HYBRIDSE_VERSION_MAJOR << "." << HYBRIDSE_VERSION_MINOR << "."
              << HYBRIDSE_VERSION_BUG << std::endl;
    cmd_client_db.name = "";
    std::string log = "hybridse";
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
        char *line = ::hybridse::base::linenoise(cmd_mode ? prefix.c_str() : continue_prefix.c_str());
        if (line == NULL) {
            return;
        }
        if (line[0] != '\0' && line[0] != '/') {
            buf.assign(line);
            if (!buf.empty()) {
                ::hybridse::base::linenoiseHistoryAdd(line);
            }
        }
        ::hybridse::base::linenoiseFree(line);
        if (buf.empty()) {
            continue;
        }

        cmd_str.append(buf);
        // TODO(CHENJING) remove
        if (cmd_str.back() == ';') {
            ::hybridse::sdk::Status status;
            HandleSqlScript(cmd_str, status);
            if (0 != status.code) {
                std::cout << "ERROR[" << status.code << "]: " << status.msg << std::endl;
            }
            cmd_str.clear();
            cmd_mode = true;
        } else {
            cmd_str.append("\n");
            cmd_mode = false;
        }
    }
}

void PrintResultSet(std::ostream &stream, ::hybridse::sdk::ResultSet *result_set) {
    if (!result_set || result_set->Size() == 0) {
        stream << "Empty set" << std::endl;
        return;
    }
    ::hybridse::base::TextTable t('-', '|', '+');
    const ::hybridse::sdk::Schema *schema = result_set->GetSchema();
    // Add Header
    for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
        t.add(schema->GetColumnName(i));
    }
    t.end_of_row();
    while (result_set->Next()) {
        for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
            sdk::DataType data_type = schema->GetColumnType(i);
            switch (data_type) {
                case hybridse::sdk::kTypeInt16: {
                    int16_t value = 0;
                    result_set->GetInt16(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case hybridse::sdk::kTypeInt32: {
                    int32_t value = 0;
                    result_set->GetInt32(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case hybridse::sdk::kTypeInt64: {
                    int64_t value = 0;
                    result_set->GetInt64(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case hybridse::sdk::kTypeFloat: {
                    float value = 0;
                    result_set->GetFloat(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case hybridse::sdk::kTypeDouble: {
                    double value = 0;
                    result_set->GetDouble(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case hybridse::sdk::kTypeString: {
                    std::string val;
                    result_set->GetString(i, &val);
                    t.add(val);
                    break;
                }
                default: {
                    t.add("NA");
                }
            }
        }
        t.end_of_row();
    }
    stream << t << std::endl;
    stream << result_set->Size() << " rows in set" << std::endl;
}

void PrintTableSchema(std::ostream &stream, const std::shared_ptr<hybridse::sdk::Schema> &schema) {
    if (nullptr == schema || schema->GetColumnCnt() == 0) {
        stream << "Empty set" << std::endl;
        return;
    }

    uint32_t items_size = schema->GetColumnCnt();
    ::hybridse::base::TextTable t('-', '|', '+');
    t.add("Field");
    t.add("Type");
    t.add("Null");
    t.end_of_row();

    for (uint32_t i = 0; i < items_size; i++) {
        t.add(schema->GetColumnName(i));
        t.add(hybridse::sdk::DataTypeName(schema->GetColumnType(i)));
        t.add(schema->IsColumnNotNull(i) ? "YES" : "NO");
        t.end_of_row();
    }

    stream << t;
    if (items_size > 1) {
        stream << items_size << " rows in set" << std::endl;
    } else {
        stream << items_size << " row in set" << std::endl;
    }
}

void PrintItems(std::ostream &stream, const std::string &head, const std::vector<std::string> &items) {
    if (items.empty()) {
        stream << "Empty set" << std::endl;
        return;
    }

    ::hybridse::base::TextTable t('-', '|', '+');
    t.add(head);
    t.end_of_row();
    for (auto item : items) {
        t.add(item);
        t.end_of_row();
    }
    stream << t;
    auto items_size = items.size();
    if (items_size > 1) {
        stream << items_size << " rows in set" << std::endl;
    } else {
        stream << items_size << " row in set" << std::endl;
    }
}

void HandleSqlScript(const std::string &script,
                     hybridse::sdk::Status &status) {  // NOLINT (runtime/references)
    if (!dbms_sdk) {
        dbms_sdk = ::hybridse::sdk::CreateDBMSSdk(FLAGS_toydb_endpoint);
        if (!dbms_sdk) {
            status.code = hybridse::common::kRpcError;
            status.msg = "Fail to connect to dbms";
            return;
        }
    }

    {
        hybridse::node::NodeManager node_manager;
        hybridse::base::Status sql_status;
        hybridse::node::PlanNodeList plan_trees;
        hybridse::plan::PlanAPI::CreatePlanTreeFromScript(script, plan_trees, &node_manager, sql_status);
        if (0 != sql_status.code) {
            status.code = sql_status.code;
            status.msg = sql_status.str();
            LOG(WARNING) << status.msg;
            return;
        }

        hybridse::node::PlanNode *node = plan_trees[0];

        if (nullptr == node) {
            status.msg = "fail to execute cmd: parser tree is null";
            status.code = hybridse::common::kPlanError;
            LOG(WARNING) << status.msg;
            return;
        }

        switch (node->GetType()) {
            case hybridse::node::kPlanTypeCmd: {
                hybridse::node::CmdPlanNode *cmd = dynamic_cast<hybridse::node::CmdPlanNode *>(node);
                HandleCmd(cmd, status);
                return;
            }
            case hybridse::node::kPlanTypeCreate: {
                dbms_sdk->ExecuteQuery(cmd_client_db.name, script, &status);
                return;
            }
            case hybridse::node::kPlanTypeInsert: {
                if (!table_sdk) {
                    table_sdk = ::hybridse::sdk::CreateTabletSdk(FLAGS_tablet_endpoint);
                }

                if (!table_sdk) {
                    status.code = hybridse::common::kConnError;
                    status.msg = " Fail to create tablet sdk";
                    return;
                }

                table_sdk->Insert(cmd_client_db.name, script, &status);

                if (0 != status.code) {
                    return;
                }
                std::cout << "Insert success" << std::endl;
                return;
            }
            case hybridse::node::kPlanTypeFuncDef:
            case hybridse::node::kPlanTypeExplain: {
                std::string empty;
                std::string mu_script = script;
                mu_script.replace(0u, 7u, empty);
                std::shared_ptr<::hybridse::sdk::ExplainInfo> info =
                    dbms_sdk->Explain(cmd_client_db.name, mu_script, &status);
                if (0 != status.code) {
                    return;
                }
                std::cout << info->GetPhysicalPlan() << std::endl;
                return;
            }
            case hybridse::node::kPlanTypeQuery: {
                if (!table_sdk) {
                    table_sdk = ::hybridse::sdk::CreateTabletSdk(FLAGS_tablet_endpoint);
                }

                if (!table_sdk) {
                    status.code = hybridse::common::kConnError;
                    status.msg = " Fail to create tablet sdk";
                    return;
                }
                std::shared_ptr<::hybridse::sdk::ResultSet> rs = table_sdk->Query(cmd_client_db.name, script, &status);
                if (rs) {
                    PrintResultSet(std::cout, rs.get());
                }
                return;
            }
            default: {
                status.msg =
                    "Fail to execute script with un-support type" + hybridse::node::NameOfPlanNodeType(node->GetType());
                status.code = hybridse::common::kUnSupport;
                return;
            }
        }
    }
}
void HandleCmd(const hybridse::node::CmdPlanNode *cmd_node,
               hybridse::sdk::Status &status) {  // NOLINT (runtime/references)
    if (dbms_sdk == NULL) {
        dbms_sdk = ::hybridse::sdk::CreateDBMSSdk(FLAGS_toydb_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    std::string db = cmd_client_db.name;
    switch (cmd_node->GetCmdType()) {
        case hybridse::node::kCmdShowDatabases: {
            std::vector<std::string> names = dbms_sdk->GetDatabases(&status);
            if (status.code == 0) {
                PrintItems(std::cout, "Databases", names);
            }
            return;
        }
        case hybridse::node::kCmdShowTables: {
            std::shared_ptr<hybridse::sdk::TableSet> rs = dbms_sdk->GetTables(db, &status);
            if (status.code == 0) {
                std::ostringstream oss;
                std::vector<std::string> names;
                while (rs->Next()) {
                    names.push_back(rs->GetTable()->GetName());
                }
                PrintItems(std::cout, "Tables_In_" + cmd_client_db.name, names);
            }
            return;
        }
        case hybridse::node::kCmdDescTable: {
            std::shared_ptr<hybridse::sdk::TableSet> rs = dbms_sdk->GetTables(db, &status);
            if (rs) {
                while (rs->Next()) {
                    if (rs->GetTable()->GetName() == cmd_node->GetArgs()[0]) {
                        PrintTableSchema(std::cout, rs->GetTable()->GetSchema());
                    }
                }
            }
            break;
        }
        case hybridse::node::kCmdCreateDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            dbms_sdk->CreateDatabase(name, &status);
            if (0 == status.code) {
                std::cout << "Create database success" << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdUseDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            std::vector<std::string> names = dbms_sdk->GetDatabases(&status);
            if (status.code == 0) {
                for (uint32_t i = 0; i < names.size(); i++) {
                    if (names[i] == name) {
                        cmd_client_db.name = name;
                        std::cout << "Database changed" << std::endl;
                        return;
                    }
                }
            }

            std::cout << "Database '" << name << "' not exists" << std::endl;
            break;
        }
        case hybridse::node::kCmdExit: {
            exit(0);
        }
        default: {
            status.code = hybridse::common::kUnSupport;
            status.msg = absl::StrCat("UnSupport Cmd ", hybridse::node::CmdTypeName(cmd_node->GetCmdType()));
        }
    }
}

}  // namespace cmd
}  // namespace hybridse

int main(int argc, char *argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_role == "dbms") {
        ::hybridse::cmd::StartDBMS(argv);
    } else if (FLAGS_role == "tablet") {
        ::hybridse::cmd::StartTablet(argc, argv);
    } else if (FLAGS_role == "client") {
        ::hybridse::cmd::StartClient(argv);
    } else if (FLAGS_role == "csv") {
    } else {
        std::cout << "Start failed! FLAGS_role must be tablet, client, dbms" << std::endl;
        return 1;
    }
    return 0;
}
