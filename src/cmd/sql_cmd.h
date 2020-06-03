/*
 * sql_cmd.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#ifndef SRC_CMD_SQL_CMD_H_
#define SRC_CMD_SQL_CMD_H_
#include <algorithm>
#include <string>
#include <vector>

#include "base/linenoise.h"
#include "base/texttable.h"
#include "catalog/schema_adapter.h"
#include "gflags/gflags.h"
#include "node/node_manager.h"
#include "parser/parser.h"
#include "proto/fe_type.pb.h"
#include "sdk/cluster_sdk.h"
#include "sdk/sql_cluster_router.h"
#include "version.h"  // NOLINT

DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_bool(interactive);

namespace rtidb {
namespace cmd {

const std::string LOGO =  // NOLINT
    ""
    "  ______   _____  ___\n"
    " |  ____|  |  __ \\|  _ \\\n"
    " | |__ ___ | |  | | |_) |\n"
    " |  __/ _  \\ |  | |  _ <\n"
    " | | |  __ / |__| | |_) |\n"
    " |_|  \\___||_____/|____/\n";

const std::string VERSION = std::to_string(RTIDB_VERSION_MAJOR) +  // NOLINT
                            "." +                                  // NOLINT
                            std::to_string(RTIDB_VERSION_MEDIUM) + "." +
                            std::to_string(RTIDB_VERSION_MINOR) + "." +
                            std::to_string(RTIDB_VERSION_BUG);

std::string db = "";  // NOLINT
::rtidb::sdk::ClusterSDK *cs = NULL;
::rtidb::sdk::SQLClusterRouter *sr = NULL;

void PrintResultSet(std::ostream &stream, ::fesql::sdk::ResultSet *result_set) {
    if (!result_set || result_set->Size() == 0) {
        stream << "Empty set" << std::endl;
        return;
    }
    ::fesql::base::TextTable t('-', '|', '+');
    auto *schema = result_set->GetSchema();
    // Add Header
    for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
        t.add(schema->GetColumnName(i));
    }
    t.endOfRow();
    while (result_set->Next()) {
        for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
            auto data_type = schema->GetColumnType(i);
            switch (data_type) {
                case fesql::sdk::kTypeInt16: {
                    int16_t value = 0;
                    result_set->GetInt16(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case fesql::sdk::kTypeInt32: {
                    int32_t value = 0;
                    result_set->GetInt32(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case fesql::sdk::kTypeInt64: {
                    int64_t value = 0;
                    result_set->GetInt64(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case fesql::sdk::kTypeFloat: {
                    float value = 0;
                    result_set->GetFloat(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case fesql::sdk::kTypeDouble: {
                    double value = 0;
                    result_set->GetDouble(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case fesql::sdk::kTypeString: {
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
        t.endOfRow();
    }
    stream << t << std::endl;
    stream << result_set->Size() << " rows in set" << std::endl;
}
void PrintTableSchema(std::ostream &stream, const ::fesql::vm::Schema &schema) {
    if (schema.size() == 0) {
        stream << "Empty set" << std::endl;
        return;
    }
    uint32_t items_size = schema.size();
    ::fesql::base::TextTable t('-', '|', '+');
    t.add("Field");
    t.add("Type");
    t.add("Null");
    t.endOfRow();

    for (uint32_t i = 0; i < items_size; i++) {
        auto column = schema.Get(i);
        t.add(column.name());
        t.add(::fesql::type::Type_Name(column.type()));
        t.add(column.is_not_null() ? "NO" : "YES");
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

void HandleCmd(const fesql::node::CmdNode *cmd_node) {
    switch (cmd_node->GetCmdType()) {
        case fesql::node::kCmdShowDatabases: {
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

        case fesql::node::kCmdShowTables: {
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

        case fesql::node::kCmdDescTable: {
            if (db.empty()) {
                std::cout << "please enter database first" << std::endl;
                return;
            }
            auto table = cs->GetTableInfo(db, cmd_node->GetArgs()[0]);
            ::fesql::vm::Schema output_schema;
            ::rtidb::catalog::SchemaAdapter::ConvertSchema(
                table->column_desc_v1(), &output_schema);
            PrintTableSchema(std::cout, output_schema);
            break;
        }

        case fesql::node::kCmdCreateDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            auto ns = cs->GetNsClient();
            std::string error;
            bool ok = ns->CreateDatabase(name, error);
            if (ok) {
                std::cout << "Create database success" << std::endl;
            } else {
                std::cout << "Create database failed for " << error
                          << std::endl;
            }
            break;
        }
        case fesql::node::kCmdUseDatabase: {
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
        case fesql::node::kCmdDropTable: {
            std::string name = cmd_node->GetArgs()[0];
            std::string error;
            printf("Drop table %s? yes/no\n", name.c_str());
            std::string input;
            std::cin >> input;
            std::transform(input.begin(), input.end(), input.begin(),
                           ::tolower);
            if (input != "yes") {
                printf("'drop %s' cmd is canceled!\n", name.c_str());
                return;
            }
            auto ns = cs->GetNsClient();
            bool ok = ns->DropTable(name, error);
            if (ok) {
                std::cout << "drop ok" << std::endl;
                sr->RefreshCatalog();
            } else {
                std::cout << "failed to drop. error msg: " << error
                          << std::endl;
            }
            break;
        }
        case fesql::node::kCmdDropIndex: {
            std::string index_name = cmd_node->GetArgs()[0];
            std::string table_name = cmd_node->GetArgs()[1];
            std::string error;
            printf("Drop index %s on %s? yes/no\n", index_name.c_str(),
                   table_name.c_str());
            std::string input;
            std::cin >> input;
            std::transform(input.begin(), input.end(), input.begin(),
                           ::tolower);
            if (input != "yes") {
                printf("'Drop index %s on %s' cmd is canceled!\n",
                       index_name.c_str(), table_name.c_str());
                return;
            }
            auto ns = cs->GetNsClient();
            bool ok = ns->DeleteIndex(table_name, index_name, error);
            if (ok) {
                std::cout << "drop index ok" << std::endl;
            } else {
                std::cout << "Fail to drop index. error msg: " << error
                          << std::endl;
            }
            break;
        }
        case fesql::node::kCmdExit: {
            exit(0);
        }
        default: {
            return;
        }
    }
}

void HandleCreateIndex(const fesql::node::CreateIndexNode *create_index_node) {
    ::rtidb::common::ColumnKey column_key;
    column_key.set_index_name(create_index_node->index_name_);
    for (const auto &key : create_index_node->index_->GetKey()) {
        column_key.add_col_name(key);
    }
    column_key.add_ts_name(create_index_node->index_->GetTs());

    std::string error;
    auto ns = cs->GetNsClient();
    bool ok = ns->AddIndex(create_index_node->table_name_, column_key, error);
    if (ok) {
        std::cout << "create index ok" << std::endl;
    } else {
        std::cout << "failed to create index. error msg: " << error
                  << std::endl;
        return;
    }
}

void HandleSQL(const std::string &sql) {
    fesql::node::NodeManager node_manager;
    fesql::parser::FeSQLParser parser;
    fesql::base::Status sql_status;
    fesql::node::NodePointVector parser_trees;
    parser.parse(sql, parser_trees, &node_manager, sql_status);
    if (0 != sql_status.code) {
        std::cout << sql_status.msg << std::endl;
        return;
    }
    fesql::node::SQLNode *node = parser_trees[0];
    switch (node->GetType()) {
        case fesql::node::kCmdStmt: {
            fesql::node::CmdNode *cmd =
                dynamic_cast<fesql::node::CmdNode *>(node);
            HandleCmd(cmd);
            return;
        }
        case fesql::node::kCreateStmt: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            ::fesql::sdk::Status status;
            bool ok = sr->ExecuteDDL(db, sql, &status);
            if (!ok) {
                std::cout << "fail to execute ddl" << std::endl;
            } else {
                sr->RefreshCatalog();
            }
            return;
        }
        case fesql::node::kCreateIndexStmt: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            fesql::node::CreateIndexNode *create_index_node =
                dynamic_cast<fesql::node::CreateIndexNode *>(node);
            HandleCreateIndex(create_index_node);
            return;
        }
        case fesql::node::kInsertStmt: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            ::fesql::sdk::Status status;
            bool ok = sr->ExecuteInsert(db, sql, &status);
            if (!ok) {
                std::cout << "fail to execute insert" << std::endl;
            }
            return;
        }
        case fesql::node::kFnList:
        case fesql::node::kQuery: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            ::fesql::sdk::Status status;
            auto rs = sr->ExecuteSQL(db, sql, &status);
            if (!rs) {
                std::cout << "fail to execute query" << std::endl;
            } else {
                PrintResultSet(std::cout, rs.get());
            }
            return;
        }
        default: {
        }
    }
}

void HandleCli() {
    std::cout << LOGO << std::endl;
    std::cout << "v" << VERSION << std::endl;
    ::rtidb::sdk::ClusterOptions copt;
    copt.zk_cluster = FLAGS_zk_cluster;
    copt.zk_path = FLAGS_zk_root_path;
    cs = new ::rtidb::sdk::ClusterSDK(copt);
    bool ok = cs->Init();
    if (!ok) {
        std::cout << "Fail to connect to db" << std::endl;
        return;
    }
    sr = new ::rtidb::sdk::SQLClusterRouter(cs);
    if (!sr->Init()) {
        std::cout << "Fail to connect to db" << std::endl;
        return;
    }
    std::string ns_endpoint = cs->GetNsClient()->GetEndpoint();
    std::string display_prefix = ns_endpoint + "/" + db + "> ";
    std::string multi_line_perfix =
        std::string(display_prefix.length() - 3, ' ') + "-> ";
    std::string sql;
    bool multi_line = false;
    while (true) {
        std::string buffer;
        char *line = ::rtidb::base::linenoise(
            multi_line ? multi_line_perfix.c_str() : display_prefix.c_str());
        if (line == NULL) {
            return;
        }
        if (line[0] != '\0' && line[0] != '/') {
            buffer.assign(line);
            boost::trim(buffer);
            if (!buffer.empty()) {
                ::rtidb::base::linenoiseHistoryAdd(line);
            }
        }
        ::rtidb::base::linenoiseFree(line);
        if (buffer.empty()) {
            continue;
        }
        sql.append(buffer);
        if (sql.back() == ';') {
            HandleSQL(sql);
            multi_line = false;
            display_prefix = ns_endpoint + "/" + db + "> ";
            multi_line_perfix =
                std::string(display_prefix.length() - 3, ' ') + "-> ";
            sql.clear();
        } else {
            sql.append("\n");
            multi_line = true;
            continue;
        }
    }
}

}  // namespace cmd
}  // namespace rtidb

#endif  // SRC_CMD_SQL_CMD_H_
