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
#ifndef EXAMPLES_TOYDB_SRC_TESTING_TOYDB_ENGINE_TEST_H_
#define EXAMPLES_TOYDB_SRC_TESTING_TOYDB_ENGINE_TEST_H_

#include "testing/toydb_test_base.h"
#include <vector>
#include <map>
#include <memory>
#include <string>
#include <set>
#include "vm/engine_test.h"
namespace fesql {
namespace vm {
class ToydbBatchEngineTestRunner : public BatchEngineTestRunner {
 public:
    explicit ToydbBatchEngineTestRunner(const SQLCase& sql_case,
                                        const EngineOptions options)
        : BatchEngineTestRunner(sql_case, options), catalog_() {}
    bool InitEngineCatalog() override {
        catalog_ = BuildToydbCatalog();
        engine_ = std::make_shared<Engine>(catalog_, options_);
        return InitToydbEngineCatalog(sql_case_, options_, name_table_map_,
                                      engine_, catalog_);
    };
    bool InitTable(const std::string table_name) override {
        auto table = name_table_map_[table_name];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Init();
    }
    bool AddRowsIntoTable(const std::string table_name,
                          const std::vector<Row>& rows) override {
        auto table = name_table_map_[table_name];
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
    bool AddRowIntoTable(const std::string table_name,
                         const Row& row) override {
        auto table = name_table_map_[table_name];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Put(reinterpret_cast<char*>(row.buf()), row.size());
    }

 private:
    std::shared_ptr<tablet::TabletCatalog> catalog_;
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map_;
};

class ToydbRequestEngineTestRunner : public RequestEngineTestRunner {
 public:
    explicit ToydbRequestEngineTestRunner(const SQLCase& sql_case,
                                          const EngineOptions options)
        : RequestEngineTestRunner(sql_case, options), catalog_() {}
    bool InitEngineCatalog() override {
        catalog_ = BuildToydbCatalog();
        engine_ = std::make_shared<Engine>(catalog_, options_);
        return InitToydbEngineCatalog(sql_case_, options_, name_table_map_,
                                      engine_, catalog_);
    };
    bool InitTable(const std::string table_name) override {
        auto table = name_table_map_[table_name];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Init();
    }
    bool AddRowsIntoTable(const std::string table_name,
                          const std::vector<Row>& rows) override {
        auto table = name_table_map_[table_name];
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
    bool AddRowIntoTable(const std::string table_name,
                         const Row& row) override {
        auto table = name_table_map_[table_name];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Put(reinterpret_cast<char*>(row.buf()), row.size());
    }

 private:
    std::shared_ptr<tablet::TabletCatalog> catalog_;
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map_;
};

class ToydbBatchRequestEngineTestRunner : public BatchRequestEngineTestRunner {
 public:
    ToydbBatchRequestEngineTestRunner(
        const SQLCase& sql_case, const EngineOptions options,
        const std::set<size_t>& common_column_indices)
        : BatchRequestEngineTestRunner(sql_case, options,
                                       common_column_indices),
          catalog_() {}
    bool InitEngineCatalog() override {
        catalog_ = BuildToydbCatalog();
        engine_ = std::make_shared<Engine>(catalog_, options_);
        return InitToydbEngineCatalog(sql_case_, options_, name_table_map_,
                                      engine_, catalog_);
    };
    bool InitTable(const std::string table_name) override {
        auto table = name_table_map_[table_name];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Init();
    }
    bool AddRowsIntoTable(const std::string table_name,
                          const std::vector<Row>& rows) override {
        LOG(INFO) << "Add rows into table " << table_name;
        auto table = name_table_map_[table_name];
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
    bool AddRowIntoTable(const std::string table_name,
                         const Row& row) override {
        auto table = name_table_map_[table_name];
        if (!table) {
            LOG(WARNING) << "table " << table_name << "not exist ";
            return false;
        }
        return table->Put(reinterpret_cast<char*>(row.buf()), row.size());
    }

 private:
    std::shared_ptr<tablet::TabletCatalog> catalog_;
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map_;
};

void BatchRequestEngineCheckWithCommonColumnIndices(
    const SQLCase& sql_case, const EngineOptions options,
    const std::set<size_t>& common_column_indices) {
    std::ostringstream oss;
    for (size_t index : common_column_indices) {
        oss << index << ",";
    }
    LOG(INFO) << "BatchRequestEngineCheckWithCommonColumnIndices: "
                 "common_column_indices = ["
              << oss.str() << "]";
    ToydbBatchRequestEngineTestRunner engine_test(sql_case, options,
                                                  common_column_indices);
    engine_test.RunCheck();
}

void BatchRequestEngineCheck(const SQLCase& sql_case,
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

void EngineCheck(const SQLCase& sql_case, const EngineOptions& options,
                 EngineMode engine_mode) {
    if (engine_mode == kBatchMode) {
        ToydbBatchEngineTestRunner engine_test(sql_case, options);
        engine_test.RunCheck();
        engine_test.RunSQLiteCheck();
    } else if (engine_mode == kRequestMode) {
        ToydbRequestEngineTestRunner engine_test(sql_case, options);
        engine_test.RunCheck();
    } else if (engine_mode == kBatchRequestMode) {
        BatchRequestEngineCheck(sql_case, options);
    }
}

}  // namespace vm
}  // namespace fesql
#endif  // EXAMPLES_TOYDB_SRC_TESTING_TOYDB_ENGINE_TEST_H_
