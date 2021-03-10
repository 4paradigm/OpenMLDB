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
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace fesql {
namespace vm {
using fesql::sqlcase::CaseDataMock;
bool AddTable(const std::shared_ptr<tablet::TabletCatalog>& catalog,
              const fesql::type::TableDef& table_def,
              std::shared_ptr<fesql::storage::Table> table);
bool AddTable(const std::shared_ptr<tablet::TabletCatalog>& catalog,
              const fesql::type::TableDef& table_def,
              std::shared_ptr<fesql::storage::Table> table, Engine* engine);

bool AddTable(const std::shared_ptr<tablet::TabletCatalog>& catalog,
              const fesql::type::TableDef& table_def,
              std::shared_ptr<fesql::storage::Table> table) {
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
              const fesql::type::TableDef& table_def,
              std::shared_ptr<fesql::storage::Table> table, Engine* engine) {
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
    SQLCase& sql_case,  // NOLINT
    const EngineOptions& engine_options,
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>&  // NOLINT
    name_table_map,                                               // NOLINT
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

        std::shared_ptr<::fesql::storage::Table> table(
            new ::fesql::storage::Table(i + 1, 1, table_def));
        if (!table->Init()) {
            LOG(WARNING) << "Fail to init toydb storage table";
            return false;
        }
        name_table_map[table_def.name()] = table;
        if (engine_options.is_cluster_optimzied()) {
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
    const fesql::type::TableDef& table_def,
    std::shared_ptr<fesql::storage::Table> table) {
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
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col0");
    index->set_second_key("col5");

    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));

    table->Init();

    auto catalog = BuildCommonCatalog(table_def, table);
    for (auto row : buffer) {
        table->Put(reinterpret_cast<char*>(row.buf()), row.size());
    }
    return catalog;
}
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
