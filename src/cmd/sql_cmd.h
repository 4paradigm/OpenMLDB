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

#ifndef SRC_CMD_SQL_CMD_H_
#define SRC_CMD_SQL_CMD_H_
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../../hybridse/include/node/node_enum.h"
#include "absl/strings/match.h"
#include "base/ddl_parser.h"
#include "base/file_util.h"
#include "base/linenoise.h"
#include "boost/regex.hpp"
#include "client/taskmanager_client.h"
#include "cmd/display.h"
#include "cmd/file_option_parser.h"
#include "cmd/split.h"
#include "codec/schema_codec.h"
#include "gflags/gflags.h"
#include "node/node_manager.h"
#include "plan/plan_api.h"
#include "proto/fe_type.pb.h"
#include "schema/index_util.h"
#include "schema/schema_adapter.h"
#include "sdk/db_sdk.h"
#include "sdk/node_adapter.h"
#include "sdk/sql_cluster_router.h"
#include "version.h"  // NOLINT
DEFINE_string(database, "", "Set database");
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_bool(interactive);
DECLARE_string(cmd);
// stand-alone mode
DECLARE_string(host);
DECLARE_int32(port);
DECLARE_int32(request_timeout_ms);

// TODO(zekai): add sql_cmd.cc
namespace openmldb::cmd {
using hybridse::plan::PlanAPI;
const std::string LOGO =  // NOLINT

    "  _____                    ______  _       _____   ______   \n"
    " / ___ \\                  |  ___ \\| |     (____ \\ (____  \\  \n"
    "| |   | |____   ____ ____ | | _ | | |      _   \\ \\ ____)  ) \n"
    "| |   | |  _ \\ / _  )  _ \\| || || | |     | |   | |  __  (  \n"
    "| |___| | | | ( (/ /| | | | || || | |_____| |__/ /| |__)  ) \n"
    " \\_____/| ||_/ \\____)_| |_|_||_||_|_______)_____/ |______/  \n"
    "        |_|                                                 \n";

const std::string VERSION = std::to_string(OPENMLDB_VERSION_MAJOR) + "." +  // NOLINT
                            std::to_string(OPENMLDB_VERSION_MINOR) + "." + std::to_string(OPENMLDB_VERSION_BUG) + "." +
                            OPENMLDB_COMMIT_ID;

std::string db = "";  // NOLINT
::openmldb::sdk::DBSDK* cs = nullptr;
::openmldb::sdk::SQLClusterRouter* sr = nullptr;
using VariableMap = std::map<std::string, std::string>;
VariableMap session_variables = {
    VariableMap::value_type("execute_mode", "online"),
    VariableMap::value_type("enable_trace", "false")
};

bool IsOnlineMode() {
    auto execute_mode = session_variables["execute_mode"];
    if (execute_mode == "online") {
        return true;
    } else if (execute_mode == "offline") {
        return false;
    } else {
        std::cout << "ERROR: unknown execute mode " << execute_mode << ", use online mode" << std::endl;
        return true;
    }
}
bool IsEnableTrace() {
    auto enable_trace = session_variables["enable_trace"];
    if (enable_trace == "true") {
        return true;
    } else if (enable_trace == "false") {
        return false;
    } else {
        std::cout << "ERROR: unknown enable_trace value " << enable_trace << ", use true|false" << std::endl;
        return true;
    }
}
void SaveResultSet(::hybridse::sdk::ResultSet* result_set, const std::string& file_path,
                   const std::shared_ptr<hybridse::node::OptionsMap>& options_map, ::openmldb::base::Status* status) {
    if (!result_set) {
        return;
    }
    openmldb::cmd::WriteFileOptionsParser options_parse;
    auto st = options_parse.Parse(options_map);
    if (!st.OK()) {
        status->msg = st.msg;
        status->code = st.code;
        return;
    }
    // Check file
    std::ofstream fstream;
    if (options_parse.GetMode() == "error_if_exists") {
        if (access(file_path.c_str(), 0) == 0) {
            status->msg = "ERROR: File already exists";
            status->code = openmldb::base::kSQLCmdRunError;
            return;
        } else {
            fstream.open(file_path);
        }
    } else if (options_parse.GetMode() == "overwrite") {
        fstream.open(file_path, std::ios::out);
    } else if (options_parse.GetMode() == "append") {
        fstream.open(file_path, std::ios::app);
        fstream << std::endl;
        if (options_parse.GetHeader()) {
            std::cout << "WARNING: In the middle of output file will have header" << std::endl;
        }
    }
    if (!fstream.is_open()) {
        status->msg = "ERROR: Failed to open file, please check file path";
        status->code = openmldb::base::kSQLCmdRunError;
        return;
    }
    // Write data
    if (options_parse.GetFormat() == "csv") {
        auto* schema = result_set->GetSchema();
        // Add Header
        if (options_parse.GetHeader()) {
            std::string schemaString;
            for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
                schemaString.append(schema->GetColumnName(i));
                if (i != schema->GetColumnCnt() - 1) {
                    schemaString += options_parse.GetDelimiter();
                }
            }
            fstream << schemaString << std::endl;
        }
        if (result_set->Size() != 0) {
            bool first = true;
            while (result_set->Next()) {
                std::string rowString;
                for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
                    if (result_set->IsNULL(i)) {
                        rowString.append(options_parse.GetNullValue());
                    } else {
                        std::string val;
                        bool ok = result_set->GetAsString(i, val);
                        if (!ok) {
                            status->msg = "ERROR: Failed to get result set value";
                            status->code = openmldb::base::kSQLCmdRunError;
                            return;
                        }
                        if (options_parse.GetQuote() != '\0' &&
                            schema->GetColumnType(i) == hybridse::sdk::kTypeString) {
                            rowString.append(options_parse.GetQuote() + val + options_parse.GetQuote());
                        } else {
                            rowString.append(val);
                        }
                    }
                    if (i != schema->GetColumnCnt() - 1) {
                        rowString += options_parse.GetDelimiter();
                    } else {
                        if (!first) {
                            fstream << std::endl;
                        } else {
                            first = false;
                        }
                        fstream << rowString;
                    }
                }
            }
        }
        status->msg = "SUCCEED: Save successfully";
    }
}

void PrintResultSet(std::ostream& stream, ::hybridse::sdk::ResultSet* result_set) {
    if (!result_set || result_set->Size() == 0) {
        stream << "Empty set" << std::endl;
        return;
    }
    ::hybridse::base::TextTable t('-', ' ', ' ');
    auto* schema = result_set->GetSchema();
    // Add Header
    for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
        t.add(schema->GetColumnName(i));
    }
    t.end_of_row();
    while (result_set->Next()) {
        for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
            if (result_set->IsNULL(i)) {
                t.add("NULL");
                continue;
            }
            t.add(result_set->GetAsStringUnsafe(i));
        }
        t.end_of_row();
    }
    stream << t << std::endl;
    stream << result_set->Size() << " rows in set" << std::endl;
}

std::shared_ptr<client::NsClient> GetAndCheckNSClient(std::string* error) {
    DCHECK(error);
    auto ns_client = cs->GetNsClient();
    if (!ns_client) {
        *error = "ERROR: Failed to connect nameserver";
    }
    return ns_client;
}

bool ParseNamesFromArgs(const std::vector<std::string>& args, std::string* db_name, std::string* sp_name,
                        std::string* error) {
    if (args.size() == 1) {
        // only sp name, no db_name
        if (db.empty()) {
            *error = "ERROR: Please enter database first";
            return false;
        }
        *db_name = db;
        *sp_name = args[0];
    } else if (args.size() == 2) {
        *db_name = args[0];
        *sp_name = args[1];
    } else {
        *error = "ERROR: Invalid args";
        return false;
    }
    return true;
}

bool CheckAnswerIfInteractive(const std::string& drop_type, const std::string& name) {
    if (FLAGS_interactive) {
        printf("Drop %s %s? yes/no\n", drop_type.c_str(), name.c_str());
        std::string input;
        std::cin >> input;
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);
        if (input != "yes") {
            printf("'Drop %s' cmd is canceled!\n", name.c_str());
            return false;
        }
    }
    return true;
}

void HandleCmd(const hybridse::node::CmdPlanNode* cmd_node) {
    std::shared_ptr<client::NsClient> ns;
    switch (cmd_node->GetCmdType()) {
        case hybridse::node::kCmdShowDatabases: {
            std::string error;
            std::vector<std::string> dbs;
            auto ok = (ns = GetAndCheckNSClient(&error)) && (ns->ShowDatabase(&dbs, error));
            if (ok) {
                PrintItemTable({"Databases"}, {dbs}, true, std::cout);
            } else {
                std::cout << error << std::endl;
            }
            return;
        }
        case hybridse::node::kCmdShowTables: {
            if (db.empty()) {
                std::cout << "ERROR: please enter database first" << std::endl;
                return;
            }
            auto tables = cs->GetTables(db);
            std::vector<std::string> table_names;
            auto it = tables.begin();
            for (; it != tables.end(); ++it) {
                table_names.push_back((*it)->name());
            }
            PrintItemTable({"Tables"}, {table_names}, true, std::cout);
            return;
        }
        case hybridse::node::kCmdDescTable: {
            std::string db_name;
            if (cmd_node->GetArgs().size() == 2) {
                db_name = cmd_node->GetArgs()[0];
            } else if (cmd_node->GetArgs().size() == 1) {
                db_name = db;
            } else {
                std::cout << "ERROR: Invalid CmdDescTable Args size" << std::endl;
                return;
            }
            if (db_name.empty()) {
                std::cout << "ERROR: Please enter database first" << std::endl;
                return;
            }

            std::string table_name = cmd_node->GetArgs().back();
            auto table = cs->GetTableInfo(db_name, table_name);
            if (table == nullptr) {
                std::cerr << "table " << db_name << "." << table_name << " does not exist" << std::endl;
                return;
            }
            PrintSchema(table->column_desc(), std::cout);
            PrintColumnKey(table->column_key(), std::cout);
            if (table->has_offline_table_info()) {
                PrintOfflineTableInfo(table->offline_table_info(), std::cout);
            }
            break;
        }
        case hybridse::node::kCmdCreateDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            std::string error;
            bool ok = (ns = GetAndCheckNSClient(&error)) && (ns->CreateDatabase(name, error));
            if (ok) {
                std::cout << "SUCCEED: Create database successfully" << std::endl;
            } else {
                std::cout << "ERROR: Create database failed for " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdUseDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            std::string error;
            bool ok = (ns = GetAndCheckNSClient(&error)) && (ns->Use(name, error));
            if (!ok) {
                std::cout << error << std::endl;
            } else {
                db = name;
                std::cout << "SUCCEED: Database changed" << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdDropDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            std::string error;
            if ((ns = GetAndCheckNSClient(&error)) && (ns->DropDatabase(name, error))) {
                std::cout << "SUCCEED: Drop successfully" << std::endl;
            } else {
                std::cout << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdDropTable: {
            std::string db_name;
            if (cmd_node->GetArgs().size() == 2) {
                db_name = cmd_node->GetArgs()[0];
            } else if (cmd_node->GetArgs().size() == 1) {
                db_name = db;
            } else {
                std::cout << "ERROR: Invalid CmdDropTable Args size" << std::endl;
                return;
            }
            if (db_name.empty()) {
                std::cout << "ERROR: Please enter database first" << std::endl;
                return;
            }
            std::string table_name = cmd_node->GetArgs().back();
            if (!CheckAnswerIfInteractive("table", table_name)) {
                return;
            }

            hybridse::sdk::Status status;
            bool ok = sr->DropTable(db_name, table_name, &status);

            if (ok) {
                std::cout << "SUCCEED: Drop successfully" << std::endl;
                sr->RefreshCatalog();
            } else {
                std::cout << "ERROR: Failed to drop, " << status.msg << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdDropIndex: {
            std::string db_name;
            std::string table_name;
            std::string index_name = cmd_node->GetArgs().back();
            if (cmd_node->GetArgs().size() == 3) {
                db_name = cmd_node->GetArgs()[0];
                table_name = cmd_node->GetArgs()[1];
            } else if (cmd_node->GetArgs().size() == 2) {
                db_name = db;
                table_name = cmd_node->GetArgs()[0];
            } else {
                std::cout << "ERROR: Invalid CmdDropIndex Args size" << std::endl;
                return;
            }
            if (db_name.empty()) {
                std::cout << "ERROR: Please enter database first" << std::endl;
                return;
            }
            if (!CheckAnswerIfInteractive("index", index_name + " on " + table_name)) {
                return;
            }
            std::string error;
            bool ok = (ns = GetAndCheckNSClient(&error)) && (ns->DeleteIndex(db_name, table_name, index_name, error));
            if (ok) {
                std::cout << "SUCCEED: Drop index successfully" << std::endl;
            } else {
                std::cout << "ERROR: Failed to drop index, " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdShowCreateSp: {
            std::string error;

            auto& args = cmd_node->GetArgs();
            std::string db_name, sp_name;
            if (!ParseNamesFromArgs(args, &db_name, &sp_name, &error)) {
                std::cout << error << std::endl;
                return;
            }

            std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info = cs->GetProcedureInfo(db_name, sp_name, &error);
            if (!sp_info) {
                std::cout << "ERROR: Failed to show procedure, " << error << std::endl;
                return;
            }
            PrintProcedureInfo(*sp_info, std::cout);
            break;
        }
        case hybridse::node::kCmdShowProcedures: {
            std::string error;
            std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> sp_infos = cs->GetProcedureInfo(&error);
            std::vector<std::vector<std::string>> lines;
            lines.reserve(sp_infos.size());
            for (auto& sp_info : sp_infos) {
                lines.push_back({sp_info->GetDbName(), sp_info->GetSpName()});
            }
            PrintItemTable({"DB", "SP"}, lines, std::cout);
            break;
        }
        case hybridse::node::kCmdDropSp: {
            if (db.empty()) {
                std::cout << "ERROR: Please enter database first" << std::endl;
                return;
            }
            std::string sp_name = cmd_node->GetArgs()[0];
            if (!CheckAnswerIfInteractive("procedure", sp_name)) {
                return;
            }
            std::string error;
            bool ok = (ns = GetAndCheckNSClient(&error)) && (ns->DropProcedure(db, sp_name, error));
            if (ok) {
                std::cout << "SUCCEED: Drop successfully" << std::endl;
            } else {
                std::cout << "ERROR: Failed to drop, " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdShowDeployment: {
            std::string error;
            std::string db_name, deploy_name;
            auto& args = cmd_node->GetArgs();
            if (!ParseNamesFromArgs(args, &db_name, &deploy_name, &error)) {
                std::cout << error << std::endl;
                return;
            }
            std::vector<api::ProcedureInfo> sps;
            auto sp = cs->GetProcedureInfo(db_name, deploy_name, &error);
            // check if deployment
            if (!sp || sp->GetType() != hybridse::sdk::kReqDeployment) {
                std::cout << (sp ? "not a deployment" : "not found") << std::endl;
                return;
            }
            PrintProcedureInfo(*sp, std::cout);
            break;
        }
        case hybridse::node::kCmdShowDeployments: {
            std::string error;
            if (db.empty()) {
                std::cout << "please enter database first" << std::endl;
                return;
            }
            // ns client get all procedures of one db
            std::vector<api::ProcedureInfo> sps;
            auto ok = (ns = GetAndCheckNSClient(&error)) && (ns->ShowProcedure(db, "", &sps, &error));
            if (!ok) {
                std::cout << error << std::endl;
                return;
            }
            std::vector<std::vector<std::string>> lines;
            for (auto& sp_info : sps) {
                if (sp_info.type() == type::kReqDeployment) {
                    lines.push_back({sp_info.db_name(), sp_info.sp_name()});
                }
            }
            PrintItemTable({"DB", "Deployment"}, lines, std::cout);
            break;
        }
        case hybridse::node::kCmdDropDeployment: {
            if (db.empty()) {
                std::cout << "ERROR: Please enter database first" << std::endl;
                return;
            }
            std::string deploy_name = cmd_node->GetArgs()[0];
            std::string error;
            // check if deployment, avoid deleting the normal procedure
            auto sp = cs->GetProcedureInfo(db, deploy_name, &error);
            if (!sp || sp->GetType() != hybridse::sdk::kReqDeployment) {
                std::cout << (sp ? "not a deployment" : "not found") << std::endl;
                return;
            }
            if (!CheckAnswerIfInteractive("deployment", deploy_name)) {
                return;
            }
            bool ok = (ns = GetAndCheckNSClient(&error)) && (ns->DropProcedure(db, deploy_name, error));
            if (ok) {
                std::cout << "SUCCEED: Drop successfully" << std::endl;
            } else {
                std::cout << "ERROR: Failed to drop. error: " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdShowSessionVariables: {
            std::vector<std::vector<std::string>> items;
            for (auto& pair : session_variables) {
                items.push_back({pair.first, pair.second});
            }
            PrintItemTable({"Variable_name", "Value"}, items, std::cout);
            break;
        }
        case hybridse::node::kCmdShowGlobalVariables: {
            std::cout << "ERROR: global variable is unsupported now" << std::endl;
            break;
        }
        case hybridse::node::kCmdExit: {
            exit(0);
        }
        case hybridse::node::kCmdShowJobs: {
            std::vector<::openmldb::taskmanager::JobInfo> job_infos;
            sr->ShowJobs(false, job_infos);
            PrintJobInfos(job_infos, std::cout);
            break;
        }
        case hybridse::node::kCmdShowJob: {
            int job_id;
            try {
                // Check argument type
                job_id = std::stoi(cmd_node->GetArgs()[0]);
            } catch (...) {
                std::cout << "ERROR: Failed to parse job id: " << cmd_node->GetArgs()[0] << std::endl;
                return;
            }

            ::openmldb::taskmanager::JobInfo job_info;
            sr->ShowJob(job_id, job_info);
            std::vector<::openmldb::taskmanager::JobInfo> job_infos;

            if (job_info.id() > 0) {
                job_infos.push_back(job_info);
            }
            PrintJobInfos(job_infos, std::cout);
            break;
        }
        case hybridse::node::kCmdStopJob: {
            int job_id;
            try {
                job_id = std::stoi(cmd_node->GetArgs()[0]);
            } catch (...) {
                std::cout << "ERROR: Failed to parse job id: " << cmd_node->GetArgs()[0] << std::endl;
                return;
            }

            ::openmldb::taskmanager::JobInfo job_info;
            sr->StopJob(job_id, job_info);

            std::vector<::openmldb::taskmanager::JobInfo> job_infos;
            if (job_info.id() > 0) {
                job_infos.push_back(job_info);
            }
            PrintJobInfos(job_infos, std::cout);
            break;
        }
        default: {
            std::cout << "ERROR: Unsupported command" << std::endl;
            return;
        }
    }
}

void HandleCreateIndex(const hybridse::node::CreateIndexNode* create_index_node) {
    ::openmldb::common::ColumnKey column_key;
    hybridse::base::Status status;
    if (!::openmldb::sdk::NodeAdapter::TransformToColumnKey(create_index_node->index_, {}, &column_key, &status)) {
        std::cout << "ERROR: Failed to create index, " << status.msg << std::endl;
        return;
    }
    // `create index` must set the index name.
    column_key.set_index_name(create_index_node->index_name_);
    DLOG(INFO) << column_key.DebugString();

    std::string error;
    auto ns = GetAndCheckNSClient(&error);
    if (!ns) {
        std::cout << error << std::endl;
        return;
    }
    bool ok = ns->AddIndex(create_index_node->table_name_, column_key, nullptr, error);
    if (ok) {
        std::cout << "SUCCEED: Create index successfully" << std::endl;
    } else {
        std::cout << "ERROR: Failed to create index, " << error << std::endl;
        return;
    }
}

base::Status HandleDeploy(const hybridse::node::DeployPlanNode* deploy_node) {
    if (db.empty()) {
        return {base::ReturnCode::kError, "please use database first"};
    }
    if (deploy_node == nullptr) {
        return {base::ReturnCode::kError, "illegal deploy statement"};
    }
    std::string select_sql = deploy_node->StmtStr() + ";";
    hybridse::vm::ExplainOutput explain_output;
    hybridse::base::Status sql_status;
    if (!cs->GetEngine()->Explain(select_sql, db, hybridse::vm::kMockRequestMode, &explain_output, &sql_status)) {
        if (IsEnableTrace()) {
            return {base::ReturnCode::kError, sql_status.str()};
        } else {
            return {base::ReturnCode::kError, sql_status.msg};
        }
    }
    // pack ProcedureInfo
    ::openmldb::api::ProcedureInfo sp_info;
    sp_info.set_db_name(db);
    sp_info.set_sp_name(deploy_node->Name());
    if (!explain_output.request_db_name.empty()) {
        sp_info.set_main_db(explain_output.request_db_name);
    } else {
        sp_info.set_main_db(db);
    }
    sp_info.set_main_table(explain_output.request_name);
    auto input_schema = sp_info.mutable_input_schema();
    auto output_schema = sp_info.mutable_output_schema();
    if (!openmldb::schema::SchemaAdapter::ConvertSchema(explain_output.input_schema, input_schema) ||
        !openmldb::schema::SchemaAdapter::ConvertSchema(explain_output.output_schema, output_schema)) {
        return {base::ReturnCode::kError, "convert schema failed"};
    }

    std::set<std::pair<std::string, std::string>> table_pair;
    ::hybridse::base::Status status;
    if (!cs->GetEngine()->GetDependentTables(select_sql, db, ::hybridse::vm::kBatchMode, &table_pair, status)) {
        return {base::ReturnCode::kError, "get dependent table failed"};
    }
    std::set<std::string> db_set;
    for (auto& table : table_pair) {
        db_set.insert(table.first);
        auto db_table = sp_info.add_tables();
        db_table->set_db_name(table.first);
        db_table->set_table_name(table.second);
    }
    if (db_set.size() > 1) {
        return {base::ReturnCode::kError, "unsupport multi database"};
    }
    std::stringstream str_stream;
    str_stream << "CREATE PROCEDURE " << deploy_node->Name() << " (";
    for (int idx = 0; idx < input_schema->size(); idx++) {
        const auto& col = input_schema->Get(idx);
        auto it = codec::DATA_TYPE_STR_MAP.find(col.data_type());
        if (it == codec::DATA_TYPE_STR_MAP.end()) {
            return {base::ReturnCode::kError, "illegal data type"};
        }
        str_stream << col.name() << " " << it->second;
        if (idx != input_schema->size() - 1) {
            str_stream << ", ";
        }
    }
    str_stream << ") BEGIN " << select_sql << " END;";

    sp_info.set_sql(str_stream.str());
    sp_info.set_type(::openmldb::type::ProcedureType::kReqDeployment);

    // extract index from sql
    std::vector<::openmldb::nameserver::TableInfo> tables;
    auto ns = cs->GetNsClient();
    // TODO(denglong): support multi db
    auto ret = ns->ShowDBTable(db, &tables);
    if (!ret.OK()) {
        return {base::ReturnCode::kError, "get table failed " + ret.msg};
    }
    std::map<std::string, ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>> table_schema_map;
    std::map<std::string, ::openmldb::nameserver::TableInfo> table_map;
    for (const auto& table : tables) {
        for (const auto& pair : table_pair) {
            if (table.name() == pair.second) {
                table_schema_map.emplace(table.name(), table.column_desc());
                table_map.emplace(table.name(), table);
                break;
            }
        }
    }
    auto index_map = base::DDLParser::ExtractIndexes(select_sql, table_schema_map);
    std::map<std::string, std::vector<::openmldb::common::ColumnKey>> new_index_map;
    for (auto& kv : index_map) {
        auto it = table_map.find(kv.first);
        if (it == table_map.end()) {
            return {base::ReturnCode::kError, "table " + kv.first + "is not exist"};
        }
        std::set<std::string> col_set;
        for (const auto& column_desc : it->second.column_desc()) {
            col_set.insert(column_desc.name());
        }
        std::set<std::string> index_id_set;
        for (const auto& column_key : it->second.column_key()) {
            index_id_set.insert(openmldb::schema::IndexUtil::GetIDStr(column_key));
        }
        int cur_index_num = it->second.column_key_size();
        int add_index_num = 0;
        std::vector<::openmldb::common::ColumnKey> new_indexs;
        for (auto& column_key : kv.second) {
            if (!column_key.has_ttl()) {
                return {base::ReturnCode::kError, "table " + kv.first + " index has not ttl"};
            }
            if (!column_key.ts_name().empty() && col_set.count(column_key.ts_name()) == 0) {
                return {base::ReturnCode::kError,
                        "ts col " + column_key.ts_name() + " is not exist in table " + kv.first};
            }
            for (const auto& col : column_key.col_name()) {
                if (col_set.count(col) == 0) {
                    return {base::ReturnCode::kError, "col " + col + " is not exist in table " + kv.first};
                }
            }
            if (index_id_set.count(openmldb::schema::IndexUtil::GetIDStr(column_key)) > 0) {
                // skip exist index
                continue;
            }
            column_key.set_index_name("INDEX_" + std::to_string(cur_index_num + add_index_num) + "_" +
                                      std::to_string(::baidu::common::timer::now_time()));
            add_index_num++;
            new_indexs.emplace_back(column_key);
        }
        if (!new_indexs.empty()) {
            if (cs->IsClusterMode()) {
                uint64_t record_cnt = 0;
                for (int idx = 0; idx < it->second.table_partition_size(); idx++) {
                    record_cnt += it->second.table_partition(idx).record_cnt();
                }
                if (record_cnt > 0) {
                    return {base::ReturnCode::kError,
                            "table " + kv.first +
                                " has online data, cannot deploy. please drop this table and create a new one"};
                }
            }
            new_index_map.emplace(kv.first, std::move(new_indexs));
        }
    }
    if (cs->IsClusterMode()) {
        for (auto& kv : new_index_map) {
            auto status = ns->AddMultiIndex(kv.first, kv.second);
            if (!status.OK()) {
                status.msg = "table " + kv.first + " add index failed. " + status.msg;
                return status;
            }
        }
    } else {
        auto tablet_accessor = cs->GetTablet();
        if (!tablet_accessor) {
            return {base::ReturnCode::kError, "cannot connect tablet"};
        }
        auto tablet_client = tablet_accessor->GetClient();
        if (!tablet_client) {
            return {base::ReturnCode::kError, "tablet client is null"};
        }
        // add index
        for (auto& kv : new_index_map) {
            auto it = table_map.find(kv.first);
            for (auto& column_key : kv.second) {
                std::vector<openmldb::common::ColumnDesc> cols;
                for (const auto& col_name : column_key.col_name()) {
                    for (const auto& col : it->second.column_desc()) {
                        if (col.name() == col_name) {
                            cols.push_back(col);
                            break;
                        }
                    }
                }
                std::string msg;
                if (!ns->AddIndex(kv.first, column_key, &cols, msg)) {
                    return {base::ReturnCode::kError, "table " + kv.first + " add index failed"};
                }
            }
        }
        // load new index data to table
        for (auto& kv : new_index_map) {
            auto it = table_map.find(kv.first);
            if (it == table_map.end()) {
                continue;
            }
            uint32_t tid = it->second.tid();
            uint32_t pid = 0;
            if (!tablet_client->ExtractMultiIndexData(tid, pid, it->second.table_partition_size(), kv.second)) {
                return {base::ReturnCode::kError, "table " + kv.first + " load data failed"};
            }
        }
    }
    return ns->CreateProcedure(sp_info, FLAGS_request_timeout_ms);
}

void HandleSet(hybridse::node::SetPlanNode* node) {
    if (node->Scope() == hybridse::node::VariableScope::kGlobalSystemVariable) {
        printf("ERROR: global system variable is unsupported\n");
        return;
    }
    auto it = session_variables.find(node->Key());
    if (it == session_variables.end()) {
        printf("ERROR: no session variable %s\n", node->Key().c_str());
        return;
    }
    session_variables[node->Key()] = node->Value()->GetExprString();
    printf("SUCCEED: OK\n");
}

template <typename T>
bool AppendColumnValue(const std::string& v, hybridse::sdk::DataType type, bool is_not_null,
                       const std::string& null_value, T row) {
    // check if null
    if (v == null_value) {
        if (is_not_null) {
            return false;
        }
        return row->AppendNULL();
    }
    try {
        switch (type) {
            case hybridse::sdk::kTypeBool: {
                bool ok = false;
                std::string b_val = v;
                std::transform(b_val.begin(), b_val.end(), b_val.begin(), ::tolower);
                if (b_val == "true") {
                    ok = row->AppendBool(true);
                } else if (b_val == "false") {
                    ok = row->AppendBool(false);
                }
                return ok;
            }
            case hybridse::sdk::kTypeInt16: {
                return row->AppendInt16(boost::lexical_cast<int16_t>(v));
            }
            case hybridse::sdk::kTypeInt32: {
                return row->AppendInt32(boost::lexical_cast<int32_t>(v));
            }
            case hybridse::sdk::kTypeInt64: {
                return row->AppendInt64(boost::lexical_cast<int64_t>(v));
            }
            case hybridse::sdk::kTypeFloat: {
                return row->AppendFloat(boost::lexical_cast<float>(v));
            }
            case hybridse::sdk::kTypeDouble: {
                return row->AppendDouble(boost::lexical_cast<double>(v));
            }
            case hybridse::sdk::kTypeString: {
                return row->AppendString(v);
            }
            case hybridse::sdk::kTypeDate: {
                std::vector<std::string> parts;
                ::openmldb::base::SplitString(v, "-", parts);
                if (parts.size() != 3) {
                    return false;
                }
                auto year = boost::lexical_cast<int32_t>(parts[0]);
                auto mon = boost::lexical_cast<int32_t>(parts[1]);
                auto day = boost::lexical_cast<int32_t>(parts[2]);
                return row->AppendDate(year, mon, day);
            }
            case hybridse::sdk::kTypeTimestamp: {
                return row->AppendTimestamp(boost::lexical_cast<int64_t>(v));
            }
            default: {
                return false;
            }
        }
    } catch (std::exception const& e) {
        return false;
    }
}

bool InsertOneRow(const std::string& database, const std::string& insert_placeholder,
                  const std::vector<int>& str_col_idx, const std::string& null_value,
                  const std::vector<std::string>& cols, std::string* error) {
    if (cols.empty()) {
        return false;
    }

    hybridse::sdk::Status status;
    auto row = sr->GetInsertRow(database, insert_placeholder, &status);
    if (!row) {
        *error = status.msg;
        return false;
    }
    // build row from cols
    auto& schema = row->GetSchema();
    auto cnt = schema->GetColumnCnt();
    if (cnt != static_cast<int>(cols.size())) {
        *error = "col size mismatch";
        return false;
    }
    // scan all strings , calc the sum, to init SQLInsertRow's string length
    std::string::size_type str_len_sum = 0;
    for (auto idx : str_col_idx) {
        if (cols[idx] != null_value) {
            str_len_sum += cols[idx].length();
        }
    }
    row->Init(static_cast<int>(str_len_sum));

    for (int i = 0; i < cnt; ++i) {
        if (!AppendColumnValue(cols[i], schema->GetColumnType(i), schema->IsColumnNotNull(i), null_value, row)) {
            *error = "translate to insert row failed";
            return false;
        }
    }

    bool ok = sr->ExecuteInsert(database, insert_placeholder, row, &status);
    if (!ok) {
        *error = "insert row failed";
        return false;
    }
    return true;
}

// Only csv format
bool HandleLoadDataInfile(const std::string& database, const std::string& table, const std::string& file_path,
                          const std::shared_ptr<hybridse::node::OptionsMap>& options, std::string* error) {
    DCHECK(error);
    std::string real_db = database.empty() ? db : database;
    if (real_db.empty()) {
        *error = "no db in sql and no use db";
        return false;
    }

    openmldb::cmd::ReadFileOptionsParser options_parse;
    auto st = options_parse.Parse(options);
    if (!st.OK()) {
        *error = st.msg;
        return false;
    }
    std::cout << "Load " << file_path << " to " << real_db << "-" << table << ", options: delimiter ["
              << options_parse.GetDelimiter() << "], has header[" << (options_parse.GetHeader() ? "true" : "false")
              << "], null_value[" << options_parse.GetNullValue() << "], format[" << options_parse.GetFormat()
              << "], quote[" << options_parse.GetQuote() << "]" << std::endl;
    // read csv
    if (!base::IsExists(file_path)) {
        *error = "file not exist";
        return false;
    }
    std::ifstream file(file_path);
    if (!file.is_open()) {
        *error = "open file failed";
        return false;
    }

    std::string line;
    if (!std::getline(file, line)) {
        *error = "read from file failed";
        return false;
    }
    std::vector<std::string> cols;
    SplitLineWithDelimiterForStrings(line, options_parse.GetDelimiter(), &cols, options_parse.GetQuote());
    auto schema = sr->GetTableSchema(real_db, table);
    if (!schema) {
        *error = "table is not exist";
        return false;
    }
    if (static_cast<int>(cols.size()) != schema->GetColumnCnt()) {
        *error = "mismatch column size";
        return false;
    }

    if (options_parse.GetHeader()) {
        // the first line is the column names, check if equal with table schema
        for (int i = 0; i < schema->GetColumnCnt(); ++i) {
            if (cols[i] != schema->GetColumnName(i)) {
                *error = "mismatch column name";
                return false;
            }
        }

        // then read the first row of data
        std::getline(file, line);
    }

    // build placeholder
    std::string holders;
    for (auto i = 0; i < schema->GetColumnCnt(); ++i) {
        holders += ((i == 0) ? "?" : ",?");
    }
    hybridse::sdk::Status status;
    std::string insert_placeholder = "insert into " + table + " values(" + holders + ");";
    std::vector<int> str_cols_idx;
    for (int i = 0; i < schema->GetColumnCnt(); ++i) {
        if (schema->GetColumnType(i) == hybridse::sdk::kTypeString) {
            str_cols_idx.emplace_back(i);
        }
    }
    uint64_t i = 0;
    do {
        cols.clear();
        SplitLineWithDelimiterForStrings(line, options_parse.GetDelimiter(), &cols, options_parse.GetQuote());
        if (!InsertOneRow(real_db, insert_placeholder, str_cols_idx, options_parse.GetNullValue(), cols, error)) {
            *error = "line [" + line + "] insert failed, " + *error;
            return false;
        }
        ++i;
    } while (std::getline(file, line));
    std::cout << "SUCCEED: Load " << i << " rows" << std::endl;
    return true;
}

void HandleSQL(const std::string& sql) {
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);

    if (0 != sql_status.code) {
        std::cout << sql_status.msg << std::endl;
        return;
    }
    hybridse::node::PlanNode* node = plan_trees[0];
    switch (node->GetType()) {
        case hybridse::node::kPlanTypeCmd: {
            auto* cmd = dynamic_cast<hybridse::node::CmdPlanNode*>(node);
            HandleCmd(cmd);
            return;
        }
        case hybridse::node::kPlanTypeExplain: {
            std::string empty;
            std::string mu_script = sql;
            mu_script.replace(0u, 7u, empty);
            ::hybridse::sdk::Status status;
            auto info = sr->Explain(db, mu_script, &status);
            if (!info) {
                std::cout << "ERROR: Failed to get explain info" << std::endl;
                return;
            }
            std::cout << info->GetPhysicalPlan() << std::endl;
            return;
        }
        case hybridse::node::kPlanTypeCreate: {
            auto create_node = dynamic_cast<hybridse::node::CreatePlanNode*>(node);
            auto status = sr->HandleSQLCreateTable(create_node, db, cs->GetNsClient());
            if (status.OK()) {
                sr->RefreshCatalog();
                std::cout << "SUCCEED: Create successfully" << std::endl;
            } else {
                std::cout << "ERROR: " << status.msg << std::endl;
            }
            return;
        }
        case hybridse::node::kPlanTypeCreateSp: {
            if (db.empty()) {
                std::cout << "ERROR: Please use database first" << std::endl;
                return;
            }
            auto create_node = dynamic_cast<hybridse::node::CreateProcedurePlanNode*>(node);
            auto status = sr->HandleSQLCreateProcedure(create_node, db, sql, cs->GetNsClient());
            if (status.OK()) {
                sr->RefreshCatalog();
                std::cout << "SUCCEED: Create successfully" << std::endl;
            } else {
                std::cout << "ERROR: " << status.msg << std::endl;
            }
            return;
        }
        case hybridse::node::kPlanTypeCreateIndex: {
            auto* create_index_node = dynamic_cast<hybridse::node::CreateIndexPlanNode*>(node);
            HandleCreateIndex(create_index_node->create_index_node_);
            return;
        }
        case hybridse::node::kPlanTypeInsert: {
            if (!IsOnlineMode()) {
                // Not support for inserting into offline storage
                std::cout << "ERROR: Can not insert in offline mode, please set @@SESSION.execute_mode='online'"
                          << std::endl;
                return;
            }

            ::hybridse::sdk::Status status;
            bool ok = sr->ExecuteInsert(db, sql, &status);
            if (!ok) {
                std::cout << "ERROR: Failed to execute insert: " << status.msg << std::endl;
                if (IsEnableTrace()) {
                    std::cout << status.trace << std::endl;
                }
            } else {
                std::cout << "SUCCEED: Insert successfully" << std::endl;
            }
            return;
        }
        case hybridse::node::kPlanTypeDeploy: {
            auto status = HandleDeploy(dynamic_cast<hybridse::node::DeployPlanNode*>(node));
            if (!status.OK()) {
                std::cout << "ERROR: " << status.msg << std::endl;
            } else {
                std::cout << "SUCCEED: deploy successfully" << std::endl;
            }
            return;
        }
        case hybridse::node::kPlanTypeFuncDef:
        case hybridse::node::kPlanTypeQuery: {
            if (IsOnlineMode()) {
                // Run online query
                ::hybridse::sdk::Status status;
                auto rs = sr->ExecuteSQL(db, sql, &status);
                if (!rs) {
                    std::cout << "ERROR: " << status.msg << std::endl;
                    if (IsEnableTrace()) {
                        std::cout << status.trace << std::endl;
                    }
                } else {
                    PrintResultSet(std::cout, rs.get());
                }
            } else {
                // Run offline query
                ::openmldb::taskmanager::JobInfo job_info;
                std::map<std::string, std::string> config;
                auto status = sr->ExecuteOfflineQuery(sql, config, db, job_info);

                std::vector<::openmldb::taskmanager::JobInfo> job_infos;
                if (status.OK() && job_info.id() > 0) {
                    job_infos.push_back(job_info);
                }
                PrintJobInfos(job_infos, std::cout);
            }
            return;
        }
        case hybridse::node::kPlanTypeSelectInto: {
            ::openmldb::base::Status status;
            if (IsOnlineMode()) {
                auto* select_into_plan_node = dynamic_cast<hybridse::node::SelectIntoPlanNode*>(node);
                const std::string& query_sql = select_into_plan_node->QueryStr();
                ::hybridse::sdk::Status sdk_status;
                auto rs = sr->ExecuteSQL(db, query_sql, &sdk_status);
                if (!rs) {
                    std::cout << "ERROR: Failed to execute SELECT INTO: " << sdk_status.msg << ")" << std::endl;
                    if (IsEnableTrace()) {
                        std::cout << sdk_status.trace << std::endl;
                    }
                    return;
                }
                const std::string& file_path = select_into_plan_node->OutFile();
                const std::shared_ptr<hybridse::node::OptionsMap> options_map = select_into_plan_node->Options();
                SaveResultSet(rs.get(), file_path, options_map, &status);
            } else {
                ::openmldb::taskmanager::JobInfo job_info;
                std::map<std::string, std::string> config;
                status = sr->ExportOfflineData(sql, config, db, job_info);
                if (status.OK() && job_info.id() > 0) {
                    PrintJobInfos({job_info}, std::cout);
                }
            }

            if (!status.OK()) {
                std::cout << "ERROR: Failed to select into " << std::endl;
            }
            std::cout << status.GetMsg() << "(" << status.GetCode() << ")" << std::endl;
            return;
        }
        case hybridse::node::kPlanTypeSet: {
            HandleSet(dynamic_cast<hybridse::node::SetPlanNode*>(node));
            return;
        }
        case hybridse::node::kPlanTypeLoadData: {
            auto plan = dynamic_cast<hybridse::node::LoadDataPlanNode*>(node);

            // Check if passes db or uses default db
            if (plan->Db().empty() && db.empty()) {
                std::cout << "ERROR: no db in sql and no default db" << std::endl;
                return;
            }

            if (cs->IsClusterMode()) {
                // Handle in cluster mode
                ::openmldb::taskmanager::JobInfo job_info;
                std::map<std::string, std::string> config;

                ::openmldb::base::Status status;
                if (IsOnlineMode()) {
                    // Handle in online mode
                    status = sr->ImportOnlineData(sql, config, db, job_info);
                } else {
                    // Handle in offline mode
                    status = sr->ImportOfflineData(sql, config, db, job_info);
                }

                std::vector<::openmldb::taskmanager::JobInfo> job_infos;
                if (status.OK() && job_info.id() > 0) {
                    job_infos.push_back(job_info);
                }
                PrintJobInfos(job_infos, std::cout);
            } else {
                // Handle in standalone mode
                std::string error;
                if (!HandleLoadDataInfile(plan->Db(), plan->Table(), plan->File(), plan->Options(), &error)) {
                    std::cout << "ERROR: Load data failed. " << error << std::endl;
                    return;
                }
            }
            return;
        }
        case hybridse::node::kPlanTypeDelete: {
            std::cout << "ERROR: delete is not supported yet" << std::endl;
            return;
        }
        default: {
            std::cout << "ERROR: Unsupported command" << std::endl;
            return;
        }
    }
}

// cluster mode: if zk_cluster is not empty,
// standalone mode:
void Shell() {
    DCHECK(cs);
    DCHECK(sr);
    if (FLAGS_interactive) {
        std::cout << LOGO << std::endl;
        std::cout << "v" << VERSION << std::endl;
    }

    std::string ns_endpoint;
    auto ns_client = cs->GetNsClient();
    if (!ns_client) {
        LOG(WARNING) << "fail to connect nameserver";
        return;
    } else {
        ns_endpoint = ns_client->GetEndpoint();
    }

    std::string display_prefix = ns_endpoint + "/" + db + "> ";
    std::string multi_line_perfix = std::string(display_prefix.length() - 3, ' ') + "-> ";
    std::string sql;
    bool multi_line = false;
    while (true) {
        std::string buffer;
        if (!FLAGS_interactive) {
            buffer = FLAGS_cmd;
            db = FLAGS_database;
            auto ns = cs->GetNsClient();
            std::string error;
            ns->Use(db, error);
        } else {
            char* line = ::openmldb::base::linenoise(multi_line ? multi_line_perfix.c_str() : display_prefix.c_str());
            if (line == nullptr) {
                return;
            }
            if (line[0] != '\0' && line[0] != '/') {
                buffer.assign(line);
                if (!buffer.empty()) {
                    ::openmldb::base::linenoiseHistoryAdd(line);
                }
            }
            ::openmldb::base::linenoiseFree(line);
            if (buffer.empty()) {
                continue;
            }
        }
        sql.append(buffer);
        if (sql.length() == 4 || sql.length() == 5) {
            if (absl::EqualsIgnoreCase(sql, "quit;") || absl::EqualsIgnoreCase(sql, "exit;") ||
                absl::EqualsIgnoreCase(sql, "quit") || absl::EqualsIgnoreCase(sql, "exit")) {
                std::cout << "Bye" << std::endl;
                return;
            }
        }
        if (sql.back() == ';') {
            HandleSQL(sql);
            multi_line = false;
            display_prefix = ns_endpoint + "/" + db + "> ";
            multi_line_perfix = std::string(display_prefix.length() - 3, ' ') + "-> ";
            sql.clear();
        } else {
            sql.append("\n");
            multi_line = true;
        }
        if (!FLAGS_interactive) {
            return;
        }
    }
}

void ClusterSQLClient() {
    ::openmldb::sdk::ClusterOptions copt;
    copt.zk_cluster = FLAGS_zk_cluster;
    copt.zk_path = FLAGS_zk_root_path;
    cs = new ::openmldb::sdk::ClusterSDK(copt);
    bool ok = cs->Init();
    if (!ok) {
        std::cout << "ERROR: Failed to connect to db" << std::endl;
        return;
    }
    sr = new ::openmldb::sdk::SQLClusterRouter(cs);
    if (!sr->Init()) {
        std::cout << "ERROR: Failed to connect to db" << std::endl;
        return;
    }
    Shell();
}

bool StandAloneInit() {
    // connect to nameserver
    if (FLAGS_host.empty() || FLAGS_port == 0) {
        std::cout << "ERROR: Host or port is missing" << std::endl;
        return false;
    }
    cs = new ::openmldb::sdk::StandAloneSDK(FLAGS_host, FLAGS_port);
    bool ok = cs->Init();
    if (!ok) {
        std::cout << "ERROR: Failed to connect to db" << std::endl;
        return false;
    }
    sr = new ::openmldb::sdk::SQLClusterRouter(cs);
    if (!sr->Init()) {
        std::cout << "ERROR: Failed to connect to db" << std::endl;
        return false;
    }
    return true;
}

void StandAloneSQLClient() {
    if (!StandAloneInit()) {
        return;
    }
    Shell();
}

}  // namespace openmldb::cmd

#endif  // SRC_CMD_SQL_CMD_H_
