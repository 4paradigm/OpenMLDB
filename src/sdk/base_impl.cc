/*
 * base_impl.cc
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

#include "sdk/base_impl.h"

namespace fesql {
namespace sdk {

static const std::string EMPTY_STR;

SchemaImpl::SchemaImpl(const vm::Schema& schema):schema_(schema){}

SchemaImpl::~SchemaImpl(){}

int32_t SchemaImpl::GetColumnCnt() const {
    return schema_.size();
}

const std::string& SchemaImpl::GetColumnName(uint32_t index) const {
    if ((int32_t)index >= schema_.size()) return EMPTY_STR;
    return schema_.Get(index).name();
}

const DataType SchemaImpl::GetColumnType(uint32_t index) const {
    if ((int32_t)index >= schema_.size()) return kTypeUnknow;
    const type::ColumnDef& column = schema_.Get(index);
    switch(column.type()) {
        case type::kBool: return kTypeBool;
        case type::kInt16: return kTypeInt16;
        case type::kInt32: return kTypeInt32;
        case type::kInt64: return kTypeInt64;
        case type::kFloat: return kTypeFloat;
        case type::kDouble: return kTypeDouble;
        case type::kDate: return kTypeDate;
        case type::kTimestamp: return kTypeTimestamp;
        default: return kTypeUnknow;
    }
}

const bool SchemaImpl::IsColumnNotNull(uint32_t index) const {
    if ((int32_t)index >= schema_.size()) return false;
    schema_.Get(index).is_not_null();
}

TableImpl::TableImpl(const type::TableDef& table_def):table_def_(table_def){
}

TableImpl::~TableImpl() {}

const std::string& TableImpl::GetName() {
    return table_def_.name();
}

const std::string& TableImpl::GetCatalog() {
    return table_def_.catalog();
}

uint64_t TableImpl::GetCreateTime() {
    return table_def_.ctime();
}

const std::unique_ptr<Schema> TableImpl::GetSchema() {
    std::unique_ptr<SchemaImpl> impl(new SchemaImpl(table_def_.columns()));
    return std::move(impl);
}

TableSetImpl::TableSetImpl(const Tables& tables):tables_(tables), index_(-1){}
TableSetImpl::~TableSetImpl() {}

bool TableSetImpl::Next() {
    index_ ++;
    if (index_ >= tables_.size()) return false;
    return true;
}

int32_t TableSetImpl::Size() {
    return tables_.size();
}

const std::unique_ptr<Table> TableSetImpl::GetTable() {
    std::unique_ptr<TableImpl> impl(new TableImpl(tables_.Get(index_)));
    return std::move(impl);
}

}  // namespace sdk
}  // namespace fesql



