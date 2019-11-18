/*
 * codec.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#include "storage/codec.h"

#include "glog/logging.h"

namespace fesql {
namespace storage {

RowBuilder::RowBuilder(const Schema* schema, 
                       int8_t* buf, uint32_t size):
                       schema_(schema), buf_(buf), 
                       size_(size), offset_(0){}

RowBuilder::~RowBuilder() {}

bool RowBuilder::Check(uint32_t delta) {
    if (offset_ + delta < size_) return true;
    return false;
}

bool RowBuilder::AppendInt32(int32_t val) {
    if (Check(4)) {
        int8_t* ptr = buf_ + offset_;
        (*(int32_t*)ptr) = val;
        offset_ += 4;
        return true;
    }
    return false;
}

bool RowBuilder::AppendInt16(int16_t val) {
    if (!Check(2))  return false;
    int8_t* ptr = buf_ + offset_;
    (*(int16_t*)ptr) = val;
    offset_ += 2;
    return true;
}

bool RowBuilder::AppendInt64(int64_t val) {
    if (!Check(8)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(int64_t*)ptr) = val;
    offset_ += 8;
    return true;
}

bool RowBuilder::AppendFloat(float val) {
    if (!Check(4)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(float*)ptr) = val;
    offset_ += 4;
    return true;
}

bool RowBuilder::AppendDouble(double val) {
    if (!Check(8)) return false;
    int8_t* ptr = buf_ + offset_;
    (*(double*)ptr) = val;
    offset_ += 8;
    return true;
}

RowView::RowView(const Schema* schema,
        const int8_t* row, uint32_t size):
    schema_(schema), row_(row), size_(size), offsets_(schema->size()) {
    uint32_t offset = 0;
    for (int32_t i = 0; i < schema_->size(); i++) {
        offsets_[i] = offset;
        const ::fesql::type::ColumnDef& column = schema_->Get(i);
        switch (column.type()) {
            case ::fesql::type::kInt16:
                {
                    offset += 2;
                    break;
                }
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat:
                {
                    offset += 4;
                    break;
                }
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble:
                {
                    offset += 8;
                    break;
                }
            default:
                {
                    LOG(WARNING) << ::fesql::type::Type_Name(column.type())  << " is not supported";
                    break;
                }
        }
    }
}

RowView::~RowView() {}

bool RowView::GetInt32(uint32_t idx, int32_t* val) {
    if (val == NULL || (int32_t)idx >= schema_->size()) {
        LOG(WARNING) << "output val is null or idx out of index";
        return false;
    }

    const ::fesql::type::ColumnDef& column = schema_->Get(idx);
    if (column.type() != ::fesql::type::kInt32) {
        LOG(WARNING) << "type mismatch required is " << ::fesql::type::Type_Name(::fesql::type::kInt32)
            << " but is " << fesql::type::Type_Name(column.type());
        return false;
    }

    uint32_t offset = offsets_[idx];
    const int8_t* ptr = row_ + offset;
    *val = *((const int32_t*)ptr);
    return true;
}

bool RowView::GetInt64(uint32_t idx, int64_t* val) {
    if (val == NULL || (int32_t)idx >= schema_->size()) {
        LOG(WARNING) << "output val is null or idx out of index";
        return false;
    }

    const ::fesql::type::ColumnDef& column = schema_->Get(idx);
    if (column.type() != ::fesql::type::kInt64) {
        LOG(WARNING) << "type mismatch required is " << ::fesql::type::Type_Name(::fesql::type::kInt64)
            << " but is " << fesql::type::Type_Name(column.type());
        return false;
    }

    uint32_t offset = offsets_[idx];
    const int8_t* ptr = row_ + offset;
    *val = *((const int64_t*)ptr);
    return true;
}

bool RowView::GetInt16(uint32_t idx, int16_t* val) {

    if (val == NULL || (int32_t)idx >= schema_->size()) {
        LOG(WARNING) << "output val is null or idx out of index";
        return false;
    }

    const ::fesql::type::ColumnDef& column = schema_->Get(idx);
    if (column.type() != ::fesql::type::kInt16) {
        LOG(WARNING) << "type mismatch required is " << ::fesql::type::Type_Name(::fesql::type::kInt16)
            << " but is " << fesql::type::Type_Name(column.type());
        return false;
    }

    uint32_t offset = offsets_[idx];
    const int8_t* ptr = row_ + offset;
    *val = *((const int16_t*)ptr);
    return true;
}

bool RowView::GetFloat(uint32_t idx, float* val) {

    if (val == NULL || (int32_t)idx >= schema_->size()) {
        LOG(WARNING) << "output val is null or idx out of index";
        return false;
    }

    const ::fesql::type::ColumnDef& column = schema_->Get(idx);
    if (column.type() != ::fesql::type::kFloat) {
        LOG(WARNING) << "type mismatch required is " << ::fesql::type::Type_Name(::fesql::type::kFloat)
            << " but is " << fesql::type::Type_Name(column.type());
        return false;
    }

    uint32_t offset = offsets_[idx];
    const int8_t* ptr = row_ + offset;
    *val = *((const float*)ptr);
    return true;
}

bool RowView::GetDouble(uint32_t idx, double* val) {

    if (val == NULL || (int32_t)idx >= schema_->size()) {
        LOG(WARNING) << "output val is null or idx out of index";
        return false;
    }

    const ::fesql::type::ColumnDef& column = schema_->Get(idx);
    if (column.type() != ::fesql::type::kDouble) {
        LOG(WARNING) << "type mismatch required is " << ::fesql::type::Type_Name(::fesql::type::kDouble)
            << " but is " << fesql::type::Type_Name(column.type());
        return false;
    }

    uint32_t offset = offsets_[idx];
    const int8_t* ptr = row_ + offset;
    *val = *((const double*)ptr);
    return true;
}

}  // namespace storage
}  // namespace fesql

