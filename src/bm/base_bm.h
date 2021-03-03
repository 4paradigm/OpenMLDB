/*
 * src/bm/base_bm.h
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

/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * base_bm.h
 *
 * Author: chenjing
 * Date: 2019/12/24
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BM_BASE_BM_H_
#define SRC_BM_BASE_BM_H_

#include <memory>
#include <random>
#include <string>
#include <vector>
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "tablet/tablet_catalog.h"
#include "vm/catalog.h"
#include "vm/mem_catalog.h"
namespace fesql {
namespace bm {
using fesql::codec::Row;
using fesql::sqlcase::SQLCase;
template <class T>
class Repeater {
 public:
    Repeater() : idx_(0), values_({}) {}
    explicit Repeater(T value) : idx_(0), values_({value}) {}
    explicit Repeater(const std::vector<T>& values)
        : idx_(0), values_(values) {}

    virtual T GetValue() {
        T value = values_[idx_];
        idx_ = (idx_ + 1) % values_.size();
        return value;
    }

    uint32_t idx_;
    std::vector<T> values_;
};

template <class T>
class NumberRepeater : public Repeater<T> {
 public:
    void Range(T min, T max, T step) {
        this->values_.clear();
        for (T v = min; v <= max; v += step) {
            this->values_.push_back(v);
        }
    }
};

template <class T>
class IntRepeater : public NumberRepeater<T> {
 public:
    void Random(T min, T max, int32_t random_size) {
        this->values_.clear();
        std::default_random_engine e;
        std::uniform_int_distribution<T> u(min, max);
        for (int i = 0; i < random_size; ++i) {
            this->values_.push_back(u(e));
        }
    }
};

template <class T>
class RealRepeater : public NumberRepeater<T> {
 public:
    void Random(T min, T max, int32_t random_size) {
        std::default_random_engine e;
        std::uniform_real_distribution<T> u(min, max);
        for (int i = 0; i < random_size; ++i) {
            this->values_.push_back(u(e));
        }
    }
};

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

void BuildTableDef(::fesql::type::TableDef& table) {  // NOLINT
    table.set_name("t1");
    table.set_catalog("db");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }
}

bool LoadResource(const std::string& resource_path,
                  type::TableDef& table_def,  // NOLINT
                  std::vector<Row>& rows) {   // NOLINT
    if (!SQLCase::LoadSchemaAndRowsFromYaml(fesql::sqlcase::FindFesqlDirPath(),
                                            resource_path, table_def, rows)) {
        return false;
    }
    return true;
}
void BuildBuf(int8_t** buf, uint32_t* size,
              ::fesql::type::TableDef& table) {  // NOLINT
    std::vector<Row> rows;
    LoadResource("cases/resource/benchmark_t1_basic_one_row.yaml", table, rows);
    *buf = rows[0].buf();
    *size = rows[0].size();
}

void BuildOnePkTableData(type::TableDef& table_def,  // NOLINT
                         std::vector<Row>& buffer,   // NOLINT
                         int64_t data_size) {
    ::fesql::bm::Repeater<std::string> col0(
        std::vector<std::string>({"hello"}));
    ::fesql::bm::IntRepeater<int32_t> col1;
    col1.Range(1, 100, 1);
    ::fesql::bm::IntRepeater<int16_t> col2;
    col2.Range(1u, 100u, 2);
    ::fesql::bm::RealRepeater<float> col3;
    col3.Range(1.0, 100.0, 3.0f);
    ::fesql::bm::RealRepeater<double> col4;
    col4.Range(100.0, 10000.0, 10.0);
    ::fesql::bm::IntRepeater<int64_t> col5;
    col5.Range(1576571615000 - 100000000, 1576571615000, 1000);
    ::fesql::bm::Repeater<std::string> col6({"astring", "bstring", "cstring",
                                             "dstring", "estring", "fstring",
                                             "gstring", "hstring"});

    BuildTableDef(table_def);
    for (int i = 0; i < data_size; ++i) {
        std::string str1 = col0.GetValue();
        std::string str2 = col6.GetValue();
        codec::RowBuilder builder(table_def.columns());
        uint32_t total_size = builder.CalTotalLength(str1.size() + str2.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str1.c_str(), str1.size());
        builder.AppendInt32(col1.GetValue());
        builder.AppendInt16(col2.GetValue());
        builder.AppendFloat(col3.GetValue());
        builder.AppendDouble(col4.GetValue());
        builder.AppendInt64(col5.GetValue());
        builder.AppendString(str2.c_str(), str2.size());
        buffer.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
}
std::shared_ptr<tablet::TabletCatalog> BuildOnePkTableStorage(
    int32_t data_size) {
    DLOG(INFO) << "insert window data";
    type::TableDef table_def;
    std::vector<Row> buffer;
    BuildOnePkTableData(table_def, buffer, data_size);
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

}  // namespace bm
}  // namespace fesql

#endif  // SRC_BM_BASE_BM_H_
