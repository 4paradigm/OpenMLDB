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
#ifndef HYBRIDSE_EXAMPLES_TOYDB_SRC_TESTING_TOYDB_ENGINE_TEST_BASE_H_
#define HYBRIDSE_EXAMPLES_TOYDB_SRC_TESTING_TOYDB_ENGINE_TEST_BASE_H_

#include <sqlite3.h>

#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/substitute.h"
#include "case/case_data_mock.h"
#include "case/sql_case.h"
#include "glog/logging.h"
#include "tablet/tablet_catalog.h"
#include "testing/engine_test_base.h"
namespace hybridse {
namespace vm {
std::shared_ptr<tablet::TabletCatalog> BuildToydbCatalog();
std::shared_ptr<tablet::TabletCatalog> BuildCommonCatalog(const hybridse::type::TableDef& table_def,
                                                          std::shared_ptr<hybridse::storage::Table> table);
bool InitToydbEngineCatalog(SqlCase& sql_case,  // NOLINT
                            const EngineOptions& engine_options,
                            std::map<std::pair<std::string, std::string>,
                                     std::shared_ptr<::hybridse::storage::Table>>&  // NOLINT
                                name_table_map,                                     // NOLINT
                            std::shared_ptr<vm::Engine> engine, std::shared_ptr<tablet::TabletCatalog> catalog);
std::shared_ptr<tablet::TabletCatalog> BuildOnePkTableStorage(int32_t data_size);
void BatchRequestEngineCheckWithCommonColumnIndices(const SqlCase& sql_case, const EngineOptions options,
                                                    const std::set<size_t>& common_column_indices);
void BatchRequestEngineCheck(const SqlCase& sql_case, const EngineOptions options);
void EngineCheck(const SqlCase& sql_case, const EngineOptions& options, EngineMode engine_mode);

int GenerateSqliteTestStringCallback(void* s, int argc, char** argv, char** azColName);
void CheckSqliteCompatible(const SqlCase& sql_case, const vm::Schema& schema, const std::vector<Row>& output);

class ToydbBatchEngineTestRunner : public BatchEngineTestRunner {
 public:
    explicit ToydbBatchEngineTestRunner(const SqlCase& sql_case, const EngineOptions options)
        : BatchEngineTestRunner(sql_case, options), catalog_() {}
    bool InitEngineCatalog() override {
        catalog_ = BuildToydbCatalog();
        engine_ = std::make_shared<Engine>(catalog_, options_);
        return InitToydbEngineCatalog(sql_case_, options_, name_table_map_, engine_, catalog_);
    };
    bool InitTable(const std::string& db, const std::string& table_name) override {
        auto table = name_table_map_[std::make_pair(db, table_name)];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Init();
    }
    bool AddRowsIntoTable(const std::string& db, const std::string& table_name, const std::vector<Row>& rows) override {
        auto table = name_table_map_[std::make_pair(db, table_name)];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        for (auto row : rows) {
            if (!table->Put(reinterpret_cast<char*>(row.buf()), row.size())) {
                return false;
            }
        }
        return true;
    }
    bool AddRowIntoTable(const std::string& db, const std::string& table_name, const Row& row) override {
        auto table = name_table_map_[std::make_pair(db, table_name)];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Put(reinterpret_cast<char*>(row.buf()), row.size());
    }

    void RunSqliteCheck() {
        // Determine whether to compare with Sqlite
        if (sql_case_.standard_sql() && sql_case_.standard_sql_compatible()) {
            std::vector<Row> output_rows;
            ASSERT_TRUE(Compute(&output_rows).isOK());
            CheckSqliteCompatible(sql_case_, GetSession()->GetSchema(), output_rows);
        }
    }
    const type::TableDef* GetTableDef(absl::string_view db, absl::string_view table) override {
        auto it = name_table_map_.find(std::make_pair(std::string(db), std::string(table)));
        if (it == name_table_map_.end()) {
            return nullptr;
        }

        return &it->second->GetTableDef();
    }

 private:
    std::shared_ptr<tablet::TabletCatalog> catalog_;
    std::map<std::pair<std::string, std::string>, std::shared_ptr<::hybridse::storage::Table>> name_table_map_;
};

class ToydbRequestEngineTestRunner : public RequestEngineTestRunner {
 public:
    explicit ToydbRequestEngineTestRunner(const SqlCase& sql_case, const EngineOptions options)
        : RequestEngineTestRunner(sql_case, options), catalog_() {}
    bool InitEngineCatalog() override {
        catalog_ = BuildToydbCatalog();
        engine_ = std::make_shared<Engine>(catalog_, options_);
        return InitToydbEngineCatalog(sql_case_, options_, name_table_map_, engine_, catalog_);
    };
    bool InitTable(const std::string& db, const std::string& table_name) override {
        auto table = name_table_map_[std::make_pair(db, table_name)];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Init();
    }
    bool AddRowsIntoTable(const std::string& db, const std::string& table_name, const std::vector<Row>& rows) override {
        auto table = name_table_map_[std::make_pair(db, table_name)];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        for (auto row : rows) {
            if (!table->Put(reinterpret_cast<char*>(row.buf()), row.size())) {
                return false;
            }
        }
        return true;
    }
    bool AddRowIntoTable(const std::string& db, const std::string& table_name, const Row& row) override {
        auto table = name_table_map_[std::make_pair(db, table_name)];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Put(reinterpret_cast<char*>(row.buf()), row.size());
    }

    const type::TableDef* GetTableDef(absl::string_view db, absl::string_view table) override {
        auto it = name_table_map_.find(std::make_pair(std::string(db), std::string(table)));
        if (it == name_table_map_.end()) {
            return nullptr;
        }

        return &it->second->GetTableDef();
    }

 private:
    std::shared_ptr<tablet::TabletCatalog> catalog_;
    std::map<std::pair<std::string, std::string>, std::shared_ptr<::hybridse::storage::Table>> name_table_map_;
};

class ToydbBatchRequestEngineTestRunner : public BatchRequestEngineTestRunner {
 public:
    ToydbBatchRequestEngineTestRunner(const SqlCase& sql_case, const EngineOptions options,
                                      const std::set<size_t>& common_column_indices)
        : BatchRequestEngineTestRunner(sql_case, options, common_column_indices), catalog_() {}
    bool InitEngineCatalog() override {
        catalog_ = BuildToydbCatalog();
        engine_ = std::make_shared<Engine>(catalog_, options_);
        return InitToydbEngineCatalog(sql_case_, options_, name_table_map_, engine_, catalog_);
    };
    bool InitTable(const std::string& db, const std::string& table_name) override {
        auto table = name_table_map_[std::make_pair(db, table_name)];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Init();
    }
    bool AddRowsIntoTable(const std::string& db, const std::string& table_name, const std::vector<Row>& rows) override {
        LOG(INFO) << "Add rows into table " << table_name;
        auto table = name_table_map_[std::make_pair(db, table_name)];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        for (auto row : rows) {
            if (!table->Put(reinterpret_cast<char*>(row.buf()), row.size())) {
                return false;
            }
        }
        return true;
    }
    bool AddRowIntoTable(const std::string& db, const std::string& table_name, const Row& row) override {
        auto table = name_table_map_[std::make_pair(db, table_name)];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Put(reinterpret_cast<char*>(row.buf()), row.size());
    }

    const type::TableDef* GetTableDef(absl::string_view db, absl::string_view table) override {
            auto it = name_table_map_.find(std::make_pair(std::string(db), std::string(table)));
            if (it == name_table_map_.end()) {
                return nullptr;
            }

            return &it->second->GetTableDef();
        }

 private:
    std::shared_ptr<tablet::TabletCatalog> catalog_;
    std::map<std::pair<std::string, std::string>, std::shared_ptr<::hybridse::storage::Table>> name_table_map_;
};

}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_EXAMPLES_TOYDB_SRC_TESTING_TOYDB_ENGINE_TEST_BASE_H_
