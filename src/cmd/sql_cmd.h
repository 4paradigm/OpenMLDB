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
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/linenoise.h"
#include "base/texttable.h"
#include "catalog/schema_adapter.h"
#include "cmd/split.h"
#include "gflags/gflags.h"
#include "node/node_manager.h"
#include "plan/plan_api.h"
#include "proto/fe_type.pb.h"
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

namespace openmldb::cmd {
using hybridse::plan::PlanAPI;
using ::openmldb::catalog::TTL_TYPE_MAP;
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
::openmldb::sdk::DBSDK *cs = nullptr;
::openmldb::sdk::SQLClusterRouter *sr = nullptr;

void PrintResultSet(std::ostream &stream, ::hybridse::sdk::ResultSet *result_set) {
    if (!result_set || result_set->Size() == 0) {
        stream << "Empty set" << std::endl;
        return;
    }
    ::hybridse::base::TextTable t('-', ' ', ' ');
    auto *schema = result_set->GetSchema();
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
            auto data_type = schema->GetColumnType(i);
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
                case hybridse::sdk::kTypeTimestamp: {
                    int64_t ts = 0;
                    result_set->GetTime(i, &ts);
                    t.add(std::to_string(ts));
                    break;
                }
                case hybridse::sdk::kTypeDate: {
                    int32_t year = 0;
                    int32_t month = 0;
                    int32_t day = 0;
                    std::stringstream ss;
                    result_set->GetDate(i, &year, &month, &day);
                    ss << year << "-" << month << "-" << day;
                    t.add(ss.str());
                    break;
                }
                case hybridse::sdk::kTypeBool: {
                    bool value = false;
                    result_set->GetBool(i, &value);
                    t.add(value ? "true" : "false");
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

void PrintTableIndex(std::ostream &stream, const ::hybridse::vm::IndexList &index_list) {
    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add("#");
    t.add("name");
    t.add("keys");
    t.add("ts");
    t.add("ttl");
    t.add("ttl_type");
    t.end_of_row();
    for (int i = 0; i < index_list.size(); i++) {
        const ::hybridse::type::IndexDef &index = index_list.Get(i);
        t.add(std::to_string(i + 1));
        t.add(index.name());
        t.add(index.first_keys(0));
        const std::string &ts_name = index.second_key();
        if (ts_name.empty()) {
            t.add("-");
        } else {
            t.add(index.second_key());
        }
        std::ostringstream oss;
        for (int ttl_idx = 0; ttl_idx < index.ttl_size(); ttl_idx++) {
            oss << index.ttl(ttl_idx);
            if (ttl_idx != index.ttl_size() - 1) {
                oss << "m,";
            }
        }
        t.add(oss.str());
        if (index.ttl_type() == ::hybridse::type::kTTLTimeLive) {
            t.add("kAbsolute");
        } else if (index.ttl_type() == ::hybridse::type::kTTLCountLive) {
            t.add("kLatest");
        } else if (index.ttl_type() == ::hybridse::type::kTTLTimeLiveAndCountLive) {
            t.add("kAbsAndLat");
        } else {
            t.add("kAbsOrLat");
        }
        t.end_of_row();
    }
    stream << t;
}

void PrintTableSchema(std::ostream &stream, const ::hybridse::vm::Schema &schema) {
    if (schema.empty()) {
        stream << "Empty set" << std::endl;
        return;
    }

    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add("#");
    t.add("Field");
    t.add("Type");
    t.add("Null");
    t.end_of_row();

    for (auto i = 0; i < schema.size(); i++) {
        const auto &column = schema.Get(i);
        t.add(std::to_string(i + 1));
        t.add(column.name());
        t.add(::hybridse::type::Type_Name(column.type()));
        t.add(column.is_not_null() ? "NO" : "YES");
        t.end_of_row();
    }
    stream << t;
}

void PrintItems(std::ostream &stream, const std::string &head, const std::vector<std::string> &items) {
    if (items.empty()) {
        stream << "Empty set" << std::endl;
        return;
    }

    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add(head);
    t.end_of_row();
    for (const auto &item : items) {
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

void PrintItems(const std::vector<std::pair<std::string, std::string>> &items, std::ostream &stream) {
    if (items.empty()) {
        stream << "Empty set" << std::endl;
        return;
    }

    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add("DB");
    t.add("SP");
    t.end_of_row();
    for (const auto &item : items) {
        t.add(item.first);
        t.add(item.second);
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

void PrintProcedureSchema(const std::string &head, const ::hybridse::sdk::Schema &sdk_schema, std::ostream &stream) {
    try {
        const auto &schema_impl = dynamic_cast<const ::hybridse::sdk::SchemaImpl &>(sdk_schema);
        auto &schema = schema_impl.GetSchema();
        if (schema.empty()) {
            stream << "Empty set" << std::endl;
            return;
        }

        ::hybridse::base::TextTable t('-', ' ', ' ');

        t.add("#");
        t.add("Field");
        t.add("Type");
        t.add("IsConstant");
        t.end_of_row();

        for (auto i = 0; i < schema.size(); i++) {
            const auto &column = schema.Get(i);
            t.add(std::to_string(i + 1));
            t.add(column.name());
            t.add(::hybridse::type::Type_Name(column.type()));
            t.add(column.is_constant() ? "YES" : "NO");
            t.end_of_row();
        }
        stream << t << std::endl;
    } catch (std::bad_cast &) {
        return;
    }
}

void PrintProcedureInfo(const hybridse::sdk::ProcedureInfo &sp_info) {
    std::vector<std::pair<std::string, std::string>> vec;
    std::pair<std::string, std::string> pair = std::make_pair(sp_info.GetDbName(), sp_info.GetSpName());
    vec.push_back(pair);
    PrintItems(vec, std::cout);
    std::vector<std::string> items{sp_info.GetSql()};
    PrintItems(std::cout, "SQL", items);
    PrintProcedureSchema("Input Schema", sp_info.GetInputSchema(), std::cout);
    PrintProcedureSchema("Output Schema", sp_info.GetOutputSchema(), std::cout);
}

void HandleCmd(const hybridse::node::CmdPlanNode *cmd_node) {
    switch (cmd_node->GetCmdType()) {
        case hybridse::node::kCmdShowDatabases: {
            std::string error;
            std::vector<std::string> dbs;
            auto ns = cs->GetNsClient();
            if (!ns) {
                std::cout << "Fail to connect to db" << std::endl;
            }
            ns->ShowDatabase(&dbs, error);
            PrintItems(std::cout, "Databases", dbs);
            return;
        }

        case hybridse::node::kCmdShowTables: {
            if (db.empty()) {
                std::cout << "please enter database first" << std::endl;
                return;
            }
            auto tables = cs->GetTables(db);
            std::vector<std::string> table_names;
            auto it = tables.begin();
            for (; it != tables.end(); ++it) {
                table_names.push_back((*it)->name());
            }
            PrintItems(std::cout, "Tables", table_names);
            return;
        }

        case hybridse::node::kCmdDescTable: {
            if (db.empty()) {
                std::cout << "please enter database first" << std::endl;
                return;
            }
            auto table = cs->GetTableInfo(db, cmd_node->GetArgs()[0]);
            if (table == nullptr) {
                std::cerr << "table " << cmd_node->GetArgs()[0] << " does not exist" << std::endl;
                return;
            }

            PrintSchema(table->column_desc());
            PrintColumnKey(table->column_key());
            break;
        }

        case hybridse::node::kCmdCreateDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            auto ns = cs->GetNsClient();
            std::string error;
            bool ok = ns->CreateDatabase(name, error);
            if (ok) {
                std::cout << "Create database success" << std::endl;
            } else {
                std::cout << "Create database failed for " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdUseDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            std::string error;
            auto ns = cs->GetNsClient();
            bool ok = ns->Use(name, error);
            if (!ok) {
                std::cout << error << std::endl;
            } else {
                db = name;
                std::cout << "Database changed" << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdDropDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            std::string error;
            auto ns = cs->GetNsClient();
            if (!ns->DropDatabase(name, error)) {
                std::cout << error << std::endl;
            } else {
                std::cout << "drop ok" << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdDropTable: {
            if (db.empty()) {
                std::cout << "please enter database first" << std::endl;
                return;
            }
            std::string name = cmd_node->GetArgs()[0];
            if (FLAGS_interactive) {
                printf("Drop table %s? yes/no\n", name.c_str());
                std::string input;
                std::cin >> input;
                std::transform(input.begin(), input.end(), input.begin(), ::tolower);
                if (input != "yes") {
                    printf("'drop %s' cmd is canceled!\n", name.c_str());
                    return;
                }
            }
            auto ns = cs->GetNsClient();
            std::string error;
            bool ok = ns->DropTable(name, error);
            if (ok) {
                std::cout << "drop ok" << std::endl;
                sr->RefreshCatalog();
            } else {
                std::cout << "failed to drop. error msg: " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdDropIndex: {
            std::string index_name = cmd_node->GetArgs()[0];
            std::string table_name = cmd_node->GetArgs()[1];
            std::string error;
            printf("Drop index %s on %s? yes/no\n", index_name.c_str(), table_name.c_str());
            std::string input;
            std::cin >> input;
            std::transform(input.begin(), input.end(), input.begin(), ::tolower);
            if (input != "yes") {
                printf("'Drop index %s on %s' cmd is canceled!\n", index_name.c_str(), table_name.c_str());
                return;
            }
            auto ns = cs->GetNsClient();
            bool ok = ns->DeleteIndex(table_name, index_name, error);
            if (ok) {
                std::cout << "drop index ok" << std::endl;
            } else {
                std::cout << "Fail to drop index. error msg: " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdShowCreateSp: {
            auto &args = cmd_node->GetArgs();
            std::string db_name, sp_name;
            if (args.size() == 1) {
                // only sp name, no db_name
                if (db.empty()) {
                    std::cout << "please enter database first" << std::endl;
                    return;
                } else {
                    db_name = db;
                }
                sp_name = args[0];
            } else if (args.size() == 2) {
                db_name = args[0];
                sp_name = args[1];
            } else {
                std::cout << "invalid args for show create procedure" << std::endl;
                return;
            }

            std::string error;
            std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info = cs->GetProcedureInfo(db_name, sp_name, &error);
            if (!sp_info) {
                std::cout << "Fail to show procdure. error msg: " << error << std::endl;
                return;
            }
            PrintProcedureInfo(*sp_info);
            break;
        }
        case hybridse::node::kCmdShowProcedures: {
            std::string error;
            std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> sp_infos = cs->GetProcedureInfo(&error);
            std::vector<std::pair<std::string, std::string>> pairs;
            pairs.reserve(sp_infos.size());
            for (auto &sp_info : sp_infos) {
                pairs.emplace_back(sp_info->GetDbName(), sp_info->GetSpName());
            }
            PrintItems(pairs, std::cout);
            break;
        }
        case hybridse::node::kCmdDropSp: {
            if (db.empty()) {
                std::cout << "please enter database first" << std::endl;
                return;
            }
            std::string sp_name = cmd_node->GetArgs()[0];
            std::string error;
            printf("Drop store procedure %s? yes/no\n", sp_name.c_str());
            std::string input;
            std::cin >> input;
            std::transform(input.begin(), input.end(), input.begin(), ::tolower);
            if (input != "yes") {
                printf("'drop %s' cmd is canceled!\n", sp_name.c_str());
                return;
            }
            auto ns = cs->GetNsClient();
            bool ok = ns->DropProcedure(db, sp_name, error);
            if (ok) {
                std::cout << "drop ok" << std::endl;
            } else {
                std::cout << "failed to drop. error msg: " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdExit: {
            exit(0);
        }
        default: {
            return;
        }
    }
}

void HandleCreateIndex(const hybridse::node::CreateIndexNode *create_index_node) {
    ::openmldb::common::ColumnKey column_key;
    hybridse::base::Status status;
    if (!::openmldb::sdk::NodeAdapter::TransformToColumnKey(create_index_node->index_, {}, &column_key, &status)) {
        std::cout << "failed to create index. error msg: " << status.msg << std::endl;
        return;
    }
    // `create index` must set the index name.
    column_key.set_index_name(create_index_node->index_name_);
    DLOG(INFO) << column_key.DebugString();

    std::string error;
    auto ns = cs->GetNsClient();
    bool ok = ns->AddIndex(create_index_node->table_name_, column_key, nullptr, error);
    if (ok) {
        std::cout << "create index ok" << std::endl;
    } else {
        std::cout << "failed to create index. error msg: " << error << std::endl;
        return;
    }
}

bool SetOption(const std::shared_ptr<hybridse::node::OptionsMap> &options, const std::string &option_name,
               hybridse::node::DataType option_type,
               std::function<bool(const hybridse::node::ConstNode *node)> const &f) {
    auto it = options->find(option_name);
    if (it == options->end()) {
        // won't set option, but no error
        return true;
    }
    auto node = it->second;
    if (node->GetDataType() != option_type) {
        std::cout << "wrong type " << hybridse::node::DataTypeName(node->GetDataType()) << " for option " << option_name
                  << std::endl;
        return false;
    }
    if (!f(node)) {
        std::cout << "parse option " << option_name << " failed" << std::endl;
        return false;
    }
    return true;
}

template <typename T>
bool AppendColumnValue(const std::string &v, hybridse::sdk::DataType type, bool is_not_null,
                       const std::string &nullValue, T row) {
    // check if null
    if (v == nullValue) {
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
            default:
                return false;
        }
    } catch (std::exception const &e) {
        return false;
    }
}

bool InsertOneRow(const std::string &insert_placeholder, const std::vector<std::string> &cols,
                  const std::string &nullValue, std::string *error) {
    if (cols.empty()) {
        return false;
    }

    hybridse::sdk::Status status;
    auto row = sr->GetInsertRow(db, insert_placeholder, &status);
    if (!row) {
        *error = status.msg;
        return false;
    }
    // build row from cols
    auto &schema = row->GetSchema();
    auto cnt = schema->GetColumnCnt();
    if (cnt != cols.size()) {
        *error = "col size mismatch";
        return false;
    }
    // scan all strings , calc the sum, to init SQLInsertRow's string length
    std::string::size_type str_len_sum = 0;
    for (int i = 0; i < cnt; ++i) {
        if (schema->GetColumnType(i) == hybridse::sdk::kTypeString && cols[i] != nullValue) {
            str_len_sum += cols[i].length();
        }
    }
    row->Init(static_cast<int>(str_len_sum));

    for (int i = 0; i < cnt; ++i) {
        if (!AppendColumnValue(cols[i], schema->GetColumnType(i), schema->IsColumnNotNull(i), nullValue, row)) {
            *error = "translate to insert row failed";
            return false;
        }
    }

    bool ok = sr->ExecuteInsert(db, insert_placeholder, row, &status);
    if (!ok) {
        *error = "insert row failed";
        return false;
    }
    return true;
}

// Only csv format
bool HandleLoadDataInfile(const std::string &database, const std::string &table, const std::string &file_path,
                          const std::shared_ptr<hybridse::node::OptionsMap> &options, std::string *error) {
    DCHECK(error);

    // options, value is ConstNode
    char delimiter = ',';
    bool header = true;
    // TODO(hw): nullValue?
    std::string nullValues{"null"}, format{"csv"};

    if (!SetOption(options, "delimiter", hybridse::node::kVarchar,
                   [&delimiter, error](const hybridse::node::ConstNode *node) {
                       auto &s = node->GetAsString();
                       if (s.size() != 1) {
                           *error = "invalid delimiter " + s;
                           return false;
                       }
                       delimiter = s[0];
                       return true;
                   }) ||
        !SetOption(options, "header", hybridse::node::kBool,
                   [&header](const hybridse::node::ConstNode *node) {
                       header = node->GetBool();
                       return true;
                   }) ||
        !SetOption(options, "nullValues", hybridse::node::kVarchar,
                   [&nullValues](const hybridse::node::ConstNode *node) {
                       nullValues = node->GetAsString();
                       return true;
                   }) ||
        !SetOption(options, "format", hybridse::node::kVarchar,
                   [&format, error](const hybridse::node::ConstNode *node) {
                       if (node->GetAsString() != "csv") {
                           *error = "only support csv";
                           return false;
                       }
                       return true;
                   })) {
        return false;
    }
    std::cout << "options: delimiter [" << delimiter << "], has header[" << header << "], nullValue[" << nullValues
              << "], format[" << format << "]" << std::endl;
    // read csv
    if (!base::IsExists(file_path)) {
        *error = "file not exist";
        return false;
    }
    std::ifstream file(file_path);
    if (!file.is_open()) {
        *error = "open failed";
        return false;
    }

    std::string line;
    if (!std::getline(file, line)) {
        *error = "read from file failed";
        return false;
    }
    std::vector<std::string> cols;
    SplitCSVLineWithDelimiterForStrings(line, delimiter, &cols);
    // build placeholder
    std::string holders;
    for (auto i = 0; i < cols.size(); ++i) {
        holders += ((i == 0) ? "?" : ",?");
    }
    hybridse::sdk::Status status;
    std::string insert_placeholder = "insert into " + table + " values(" + holders + ");";

    if (header) {
        // the first line is the column names
        // TODO(hw): check column names or build a name->idx map?

        // then read the first row of data
        std::getline(file, line);
    }

    do {
        cols.clear();
        SplitCSVLineWithDelimiterForStrings(line, delimiter, &cols);
        if (!InsertOneRow(insert_placeholder, cols, nullValues, error)) {
            *error = "line [" + line + "] insert failed, " + *error;
            return false;
        }
    } while (std::getline(file, line));
    return true;
}

void HandleSQL(const std::string &sql) {
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);

    if (0 != sql_status.code) {
        std::cout << sql_status.msg << std::endl;
        return;
    }
    hybridse::node::PlanNode *node = plan_trees[0];
    switch (node->GetType()) {
        case hybridse::node::kPlanTypeCmd: {
            auto *cmd = dynamic_cast<hybridse::node::CmdPlanNode *>(node);
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
                std::cout << "fail to get explain info" << std::endl;
                return;
            }
            std::cout << info->GetPhysicalPlan() << std::endl;
            return;
        }
        case hybridse::node::kPlanTypeCreate:
        case hybridse::node::kPlanTypeCreateSp: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            ::hybridse::sdk::Status status;
            bool ok = sr->ExecuteDDL(db, sql, &status);
            if (!ok) {
                std::cout << "fail to execute ddl" << std::endl;
            } else {
                sr->RefreshCatalog();
            }
            return;
        }
        case hybridse::node::kPlanTypeCreateIndex: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            auto *create_index_node = dynamic_cast<hybridse::node::CreateIndexPlanNode *>(node);
            HandleCreateIndex(create_index_node->create_index_node_);
            return;
        }
        case hybridse::node::kPlanTypeInsert: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            ::hybridse::sdk::Status status;
            bool ok = sr->ExecuteInsert(db, sql, &status);
            if (!ok) {
                std::cout << "fail to execute insert" << std::endl;
            }
            return;
        }
        case hybridse::node::kPlanTypeFuncDef:
        case hybridse::node::kPlanTypeQuery: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            ::hybridse::sdk::Status status;
            auto rs = sr->ExecuteSQL(db, sql, &status);
            if (!rs) {
                std::cout << "fail to execute query" << std::endl;
            } else {
                PrintResultSet(std::cout, rs.get());
            }
            return;
        }
        case hybridse::node::kPlanTypeLoadData: {
            auto plan = dynamic_cast<hybridse::node::LoadDataPlanNode *>(node);
            std::string error;
            if (!HandleLoadDataInfile(plan->Db(), plan->Table(), plan->File(), plan->Options(), &error)) {
                std::cout << "load data failed, err: " << error << std::endl;
                return;
            }
            std::cout << "load data succeed" << std::endl;
            return;
        }
        default: {
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

    std::string ns_endpoint = cs->GetNsClient()->GetEndpoint();
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
            char *line = ::openmldb::base::linenoise(multi_line ? multi_line_perfix.c_str() : display_prefix.c_str());
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
        if (sql == "quit;" || sql == "exit;" || sql == "quit" || sql == "exit") {
            std::cout << "Bye" << std::endl;
            sql.clear();
            return;
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
        std::cout << "Fail to connect to db" << std::endl;
        return;
    }
    sr = new ::openmldb::sdk::SQLClusterRouter(cs);
    if (!sr->Init()) {
        std::cout << "Fail to connect to db" << std::endl;
        return;
    }
    Shell();
}

void StandAloneSQLClient() {
    // connect to nameserver
    if (FLAGS_host.empty() || FLAGS_port == 0) {
        std::cout << "host or port is missing" << std::endl;
    }
    cs = new ::openmldb::sdk::StandAloneSDK(FLAGS_host, FLAGS_port);
    bool ok = cs->Init();
    if (!ok) {
        std::cout << "Fail to connect to db" << std::endl;
        return;
    }
    sr = new ::openmldb::sdk::SQLClusterRouter(cs);
    if (!sr->Init()) {
        std::cout << "Fail to connect to db" << std::endl;
        return;
    }
    Shell();
}

}  // namespace openmldb::cmd

#endif  // SRC_CMD_SQL_CMD_H_
