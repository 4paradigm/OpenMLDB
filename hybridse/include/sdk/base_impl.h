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

#ifndef HYBRIDSE_INCLUDE_SDK_BASE_IMPL_H_
#define HYBRIDSE_INCLUDE_SDK_BASE_IMPL_H_

#include <memory>
#include <string>
#include "sdk/base.h"
#include "vm/catalog.h"

namespace hybridse {
namespace sdk {

typedef ::google::protobuf::RepeatedPtrField< ::hybridse::type::TableDef>
    Tables;

class SchemaImpl : public Schema {
 public:
    explicit SchemaImpl(const codec::Schema& schema);
    SchemaImpl() {}

    ~SchemaImpl();

    const codec::Schema& GetSchema() const { return schema_; }
    inline void SetSchema(const codec::Schema& schema) { schema_ = schema; }
    int32_t GetColumnCnt() const;

    const std::string& GetColumnName(uint32_t index) const;

    const DataType GetColumnType(uint32_t index) const;
    const bool IsColumnNotNull(uint32_t index) const;
    const bool IsConstant(uint32_t index) const;

 private:
    codec::Schema schema_;
};

class TableImpl : public Table {
 public:
    explicit TableImpl(const type::TableDef& table_def);
    ~TableImpl();
    const std::string& GetName();
    const std::string& GetCatalog();
    uint64_t GetCreateTime();
    const std::shared_ptr<Schema> GetSchema();

 private:
    const type::TableDef& table_def_;
};

class TableSetImpl : public TableSet {
 public:
    explicit TableSetImpl(const Tables& tables);
    ~TableSetImpl();
    bool Next();
    const std::shared_ptr<Table> GetTable();
    int32_t Size();

 private:
    const Tables tables_;
    int32_t index_;
};

}  // namespace sdk
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_SDK_BASE_IMPL_H_
