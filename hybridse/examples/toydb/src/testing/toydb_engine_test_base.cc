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

#include "testing/toydb_engine_test_base.h"

#include "absl/strings/str_join.h"
#include "gtest/gtest.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace hybridse {
namespace vm {
using hybridse::sqlcase::CaseDataMock;
bool AddTable(const std::shared_ptr<tablet::TabletCatalog>& catalog,
              const hybridse::type::TableDef& table_def,
              std::shared_ptr<hybridse::storage::Table> table);
bool AddTable(const std::shared_ptr<tablet::TabletCatalog>& catalog,
              const hybridse::type::TableDef& table_def,
              std::shared_ptr<hybridse::storage::Table> table, Engine* engine);

bool AddTable(const std::shared_ptr<tablet::TabletCatalog>& catalog,
              const hybridse::type::TableDef& table_def,
              std::shared_ptr<hybridse::storage::Table> table) {
    std::shared_ptr<tablet::TabletTableHandler> handler(
        new tablet::TabletTableHandler(table_def.columns(), table_def.name(),
                                       table_def.catalog(), table_def.indexes(),
                                       table));
    bool ok = handler->Init();
    if (!ok) {
        return false;
    }
    return catalog->AddTable(handler);
}

bool AddTable(const std::shared_ptr<tablet::TabletCatalog>& catalog,
              const hybridse::type::TableDef& table_def,
              std::shared_ptr<hybridse::storage::Table> table, Engine* engine) {
    auto local_tablet = std::shared_ptr<vm::Tablet>(
        new vm::LocalTablet(engine, std::shared_ptr<CompileInfoCache>()));
    std::shared_ptr<tablet::TabletTableHandler> handler(
        new tablet::TabletTableHandler(table_def.columns(), table_def.name(),
                                       table_def.catalog(), table_def.indexes(),
                                       table, local_tablet));
    bool ok = handler->Init();
    if (!ok) {
        return false;
    }
    return catalog->AddTable(handler);
}
bool InitToydbEngineCatalog(
    SqlCase& sql_case,  // NOLINT
    const EngineOptions& engine_options,
    std::map<std::pair<std::string, std::string>,
             std::shared_ptr<::hybridse::storage::Table>>&  // NOLINT
        name_table_map,                                     // NOLINT
    std::shared_ptr<vm::Engine> engine,
    std::shared_ptr<tablet::TabletCatalog> catalog) {
    LOG(INFO) << "Init Toy DB Engine & Catalog";
    for (int32_t i = 0; i < sql_case.CountInputs(); i++) {
        if (sql_case.inputs_[i].name_.empty()) {
            sql_case.inputs_[i].name_ = "auto_t" + std::to_string(i);
        }
        type::TableDef table_def;
        if (!sql_case.ExtractInputTableDef(table_def, i)) {
            return false;
        }
        table_def.set_name(sql_case.inputs_[i].name_);

        std::shared_ptr<::hybridse::storage::Table> table(
            new ::hybridse::storage::Table(i + 1, 1, table_def));
        if (!table->Init()) {
            LOG(WARNING) << "Fail to init toydb storage table";
            return false;
        }
        name_table_map[std::make_pair(table_def.catalog(), table_def.name())] = table;
        if (engine_options.IsClusterOptimzied()) {
            // add table with local tablet
            if (!AddTable(catalog, table_def, table, engine.get())) {
                return false;
            }
        } else {
            if (!AddTable(catalog, table_def, table)) {
                return false;
            }
        }
    }
    return true;
}

std::shared_ptr<tablet::TabletCatalog> BuildToydbCatalog() {
    std::shared_ptr<tablet::TabletCatalog> catalog(new tablet::TabletCatalog());
    return catalog;
}
std::shared_ptr<tablet::TabletCatalog> BuildCommonCatalog(
    const hybridse::type::TableDef& table_def,
    std::shared_ptr<hybridse::storage::Table> table) {
    std::shared_ptr<tablet::TabletCatalog> catalog(new tablet::TabletCatalog());
    bool ok = catalog->Init();
    if (!ok) {
        return std::shared_ptr<tablet::TabletCatalog>();
    }
    if (!AddTable(catalog, table_def, table)) {
        return std::shared_ptr<tablet::TabletCatalog>();
    }
    return catalog;
}

std::shared_ptr<tablet::TabletCatalog> BuildOnePkTableStorage(
    int32_t data_size) {
    DLOG(INFO) << "insert window data";
    type::TableDef table_def;
    std::vector<Row> buffer;
    CaseDataMock::BuildOnePkTableData(table_def, buffer, data_size);
    // Build index
    ::hybridse::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col0");
    index->set_second_key("col5");

    std::shared_ptr<::hybridse::storage::Table> table(
        new ::hybridse::storage::Table(1, 1, table_def));

    table->Init();

    auto catalog = BuildCommonCatalog(table_def, table);
    for (auto row : buffer) {
        table->Put(reinterpret_cast<char*>(row.buf()), row.size());
    }
    return catalog;
}
// Run check with common column index info
void BatchRequestEngineCheckWithCommonColumnIndices(const SqlCase& sql_case, const EngineOptions options,
                                                    const std::set<size_t>& common_column_indices) {
    LOG(INFO) << "BatchRequestEngineCheckWithCommonColumnIndices: common_column_indices = ["
              << absl::StrJoin(common_column_indices, ",") << "]";
    ToydbBatchRequestEngineTestRunner engine_test(sql_case, options, common_column_indices);
    engine_test.RunCheck();
}

void BatchRequestEngineCheck(const SqlCase& sql_case,
                             const EngineOptions options) {
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    if (has_batch_request) {
        BatchRequestEngineCheckWithCommonColumnIndices(
            sql_case, options, sql_case.batch_request().common_column_indices_);
    } else if (!sql_case.inputs().empty()) {
        // set different common column conf
        size_t schema_size = sql_case.inputs()[0].columns_.size();
        std::set<size_t> common_column_indices;

        // empty
        BatchRequestEngineCheckWithCommonColumnIndices(sql_case, options,
                                                       common_column_indices);

        // full
        for (size_t i = 0; i < schema_size; ++i) {
            common_column_indices.insert(i);
        }
        BatchRequestEngineCheckWithCommonColumnIndices(sql_case, options,
                                                       common_column_indices);
        common_column_indices.clear();

        // partial
        // 0, 2, 4, ...
        for (size_t i = 0; i < schema_size; i += 2) {
            common_column_indices.insert(i);
        }
        BatchRequestEngineCheckWithCommonColumnIndices(sql_case, options,
                                                       common_column_indices);
        common_column_indices.clear();
        return;
        // 1, 3, 5, ...
        for (size_t i = 1; i < schema_size; i += 2) {
            common_column_indices.insert(i);
        }
        BatchRequestEngineCheckWithCommonColumnIndices(sql_case, options,
                                                       common_column_indices);
    }
}

void EngineCheck(const SqlCase& sql_case, const EngineOptions& options,
                 EngineMode engine_mode) {
    engine_mode = hybridse::vm::Engine::TryDetermineEngineMode(sql_case.sql_str(), engine_mode);
    if (engine_mode == kBatchMode) {
        ToydbBatchEngineTestRunner engine_test(sql_case, options);
        engine_test.RunCheck();
        engine_test.RunSqliteCheck();
    } else if (engine_mode == kRequestMode) {
        ToydbRequestEngineTestRunner engine_test(sql_case, options);
        engine_test.RunCheck();
    } else if (engine_mode == kBatchRequestMode) {
        BatchRequestEngineCheck(sql_case, options);
    }
}

int GenerateSqliteTestStringCallback(void* s, int argc, char** argv,
                                     char** azColName) {
    std::string& sqliteStr = *static_cast<std::string*>(s);
    int i;
    for (i = 0; i < argc; i++) {
        sqliteStr += NULL == argv[i] ? "NULL" : argv[i];
        sqliteStr += ", ";
    }
    sqliteStr = sqliteStr.substr(0, sqliteStr.length() - 2);
    sqliteStr += "\n";
    return 0;
}

void CheckSqliteCompatible(const SqlCase& sql_case, const vm::Schema& schema,
                           const std::vector<Row>& output) {
    // Use Sqlite to get output
    sqlite3* db;
    char* zErrMsg = 0;
    int rc;

    // Create database in the memory
    rc = sqlite3_open(":memory:", &db);
    if (rc) {
        LOG(ERROR) << "Can't open database: %s\n" << sqlite3_errmsg(db);
        exit(0);
    } else {
        LOG(INFO) << "Database Create successfully\n";
    }

    // Create SQL statement to create a table schema
    type::TableDef output_table;
    sql_case.ExtractInputTableDef(output_table);
    std::string create_table_sql;
    SqlCase::BuildCreateSqlFromSchema(output_table, &create_table_sql, false);
    LOG(INFO) << create_table_sql;

    // Create a table schema
    const char* create_table_sql_ch = create_table_sql.c_str();
    rc = sqlite3_exec(db, create_table_sql_ch, 0, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        LOG(ERROR) << "SQL error: %s\n" << zErrMsg;
        sqlite3_free(zErrMsg);
    } else {
        LOG(INFO) << "Table schema created successfully\n";
    }

    // Create SQL statements to insert data to the table (One insert)
    std::string create_insert_sql = "";
    std::vector<std::string> data_line;
    sql_case.BuildInsertSqlFromInput(0, &create_insert_sql);

    // Insert data into the table
    const char* create_insert_sql_ch = create_insert_sql.c_str();
    std::cout << create_insert_sql_ch << std::endl;
    rc = sqlite3_exec(db, create_insert_sql_ch, 0, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        LOG(ERROR) << "SQL error: %s\n" << zErrMsg;
        sqlite3_free(zErrMsg);
    } else {
        LOG(INFO) << "Records created successfully\n";
    }

    // Execute SQL statement
    const char* create_execute_sql_ch = sql_case.sql_str().c_str();
    std::string sqliteStr = "";
    rc = sqlite3_exec(db, create_execute_sql_ch,
                      GenerateSqliteTestStringCallback,
                      static_cast<void*>(&sqliteStr), &zErrMsg);
    if (rc != SQLITE_OK) {
        LOG(ERROR) << "SQL error: " << zErrMsg
                   << "\nsql: " << create_execute_sql_ch;
        sqlite3_free(zErrMsg);
    } else {
        LOG(INFO) << "Operation done successfully\n";
    }
    sqlite3_close(db);
    sqliteStr.pop_back();

    // Transfer Sqlite outcome to ToyDB row
    std::vector<hybridse::codec::Row> sqliteRows;
    SqlCase::ExtractRows(schema, sqliteStr, sqliteRows);

    // Compare ToyDB output with Sqlite output.
    ASSERT_NO_FATAL_FAILURE(CheckRows(
        schema, SortRows(schema, sqliteRows, sql_case.expect().order_),
        SortRows(schema, output, sql_case.expect().order_)));
}
}  // namespace vm
}  // namespace hybridse
