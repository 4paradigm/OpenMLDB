/*
 * fe_row_selector.cc
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

#include "codec/fe_row_selector.h"
#include <string>
#include <utility>
#include "codec/type_codec.h"
#include "glog/logging.h"

namespace fesql {
namespace codec {

fesql::codec::Schema RowSelector::CreateTargetSchema() {
    Schema target_schema;
    for (auto& pair : indices_) {
        size_t schema_idx = pair.first;
        size_t col_idx = pair.second;
        if (schema_idx < schemas_.size() &&
            col_idx < static_cast<size_t>(schemas_[schema_idx]->size())) {
            *target_schema.Add() = schemas_[schema_idx]->Get(col_idx);
        }
    }
    return target_schema;
}

static auto RowSelectorMakeIndices(const std::vector<size_t>& indices) {
    std::vector<std::pair<size_t, size_t>> result;
    for (size_t idx : indices) {
        result.push_back(std::make_pair(0, idx));
    }
    return result;
}

RowSelector::RowSelector(const fesql::codec::Schema* schema,
                         const std::vector<size_t>& indices)
    : schemas_({schema}),
      indices_(RowSelectorMakeIndices(indices)),
      target_schema_(CreateTargetSchema()),
      target_row_builder_(target_schema_) {
    row_views_.push_back(RowView(*schema));
}

RowSelector::RowSelector(
    const std::vector<const fesql::codec::Schema*>& schemas,
    const std::vector<std::pair<size_t, size_t>>& indices)
    : schemas_(schemas),
      indices_(indices),
      target_schema_(CreateTargetSchema()),
      target_row_builder_(target_schema_) {
    for (auto schema : schemas) {
        row_views_.push_back(RowView(*schema));
    }
}

bool RowSelector::Select(const Row& row, int8_t** out_slice, size_t* out_size) {
    if (static_cast<size_t>(row.GetRowPtrCnt()) != row_views_.size()) {
        LOG(WARNING) << "Illegal row slices, expect " << row_views_.size()
                     << ", get " << row.GetRowPtrCnt();
        return false;
    }
    for (size_t i = 0; i < row_views_.size(); ++i) {
        row_views_[i].Reset(row.buf(i), row.size(i));
    }
    size_t str_size = 0;
    for (auto& pair : indices_) {
        size_t schema_idx = pair.first;
        size_t col_idx = pair.second;
        if (schema_idx >= row_views_.size()) {
            LOG(WARNING) << "Schema idx out of bound: " << schema_idx;
            return false;
        }
        auto schema = schemas_[schema_idx];
        auto& row_view = row_views_[schema_idx];
        if (col_idx < static_cast<size_t>(schema->size())) {
            if (schema->Get(col_idx).type() == type::kVarchar &&
                !row_view.IsNULL(col_idx)) {
                str_size += row_view.GetStringUnsafe(col_idx).size();
            }
        }
    }
    size_t target_size = target_row_builder_.CalTotalLength(str_size);
    *out_slice = reinterpret_cast<int8_t*>(malloc(target_size));
    *out_size = target_size;

    target_row_builder_.SetBuffer(*out_slice, *out_size);
    for (auto& pair : indices_) {
        size_t schema_idx = pair.first;
        size_t col_idx = pair.second;
        auto schema = schemas_[schema_idx];
        auto& row_view = row_views_[schema_idx];
        if (col_idx >= static_cast<size_t>(schema->size())) {
            continue;
        }
        if (row_view.IsNULL(col_idx)) {
            target_row_builder_.AppendNULL();
            continue;
        }
        switch (schema->Get(col_idx).type()) {
            case type::kInt16: {
                target_row_builder_.AppendInt16(
                    row_view.GetInt16Unsafe(col_idx));
                break;
            }
            case type::kInt32: {
                target_row_builder_.AppendInt32(
                    row_view.GetInt32Unsafe(col_idx));
                break;
            }
            case type::kInt64: {
                target_row_builder_.AppendInt64(
                    row_view.GetInt64Unsafe(col_idx));
                break;
            }
            case type::kBool: {
                target_row_builder_.AppendBool(row_view.GetBoolUnsafe(col_idx));
                break;
            }
            case type::kFloat: {
                target_row_builder_.AppendFloat(
                    row_view.GetFloatUnsafe(col_idx));
                break;
            }
            case type::kDouble: {
                target_row_builder_.AppendDouble(
                    row_view.GetDoubleUnsafe(col_idx));
                break;
            }
            case type::kDate: {
                int32_t year;
                int32_t month;
                int32_t day;
                row_view.GetDate(col_idx, &year, &month, &day);
                target_row_builder_.AppendDate(year, month, day);
                break;
            }
            case type::kTimestamp: {
                target_row_builder_.AppendTimestamp(
                    row_view.GetTimestampUnsafe(col_idx));
                break;
            }
            case type::kVarchar: {
                std::string str = row_view.GetStringUnsafe(col_idx);
                target_row_builder_.AppendString(str.data(), str.size());
                break;
            }
            default:
                continue;
        }
    }
    return true;
}

bool RowSelector::Select(const int8_t* slice, size_t size, int8_t** out_slice,
                         size_t* out_size) {
    if (row_views_.size() < 1) {
        LOG(WARNING) << "Empty row views";
        return false;
    }
    auto& row_view = row_views_[0];
    auto& schema = schemas_[0];
    row_view.Reset(slice, size);
    size_t str_size = 0;
    for (auto& pair : indices_) {
        size_t schema_idx = pair.first;
        size_t col_idx = pair.second;
        if (schema_idx > 0) {
            LOG(WARNING) << "Schema idx out of bound";
            return false;
        }
        if (col_idx < static_cast<size_t>(schema->size())) {
            if (schema->Get(col_idx).type() == type::kVarchar &&
                !row_view.IsNULL(col_idx)) {
                str_size += row_view.GetStringUnsafe(col_idx).size();
            }
        }
    }
    size_t target_size = target_row_builder_.CalTotalLength(str_size);
    *out_slice = reinterpret_cast<int8_t*>(malloc(target_size));
    *out_size = target_size;

    target_row_builder_.SetBuffer(*out_slice, *out_size);
    for (auto& pair : indices_) {
        size_t col_idx = pair.second;
        if (col_idx >= static_cast<size_t>(schema->size())) {
            continue;
        }
        if (row_view.IsNULL(col_idx)) {
            target_row_builder_.AppendNULL();
            continue;
        }
        switch (schema->Get(col_idx).type()) {
            case type::kInt16: {
                target_row_builder_.AppendInt16(
                    row_view.GetInt16Unsafe(col_idx));
                break;
            }
            case type::kInt32: {
                target_row_builder_.AppendInt32(
                    row_view.GetInt32Unsafe(col_idx));
                break;
            }
            case type::kInt64: {
                target_row_builder_.AppendInt64(
                    row_view.GetInt64Unsafe(col_idx));
                break;
            }
            case type::kBool: {
                target_row_builder_.AppendBool(row_view.GetBoolUnsafe(col_idx));
                break;
            }
            case type::kFloat: {
                target_row_builder_.AppendFloat(
                    row_view.GetFloatUnsafe(col_idx));
                break;
            }
            case type::kDouble: {
                target_row_builder_.AppendDouble(
                    row_view.GetDoubleUnsafe(col_idx));
                break;
            }
            case type::kDate: {
                int32_t year;
                int32_t month;
                int32_t day;
                row_view.GetDate(col_idx, &year, &month, &day);
                target_row_builder_.AppendDate(year, month, day);
                break;
            }
            case type::kTimestamp: {
                target_row_builder_.AppendTimestamp(
                    row_view.GetTimestampUnsafe(col_idx));
                break;
            }
            case type::kVarchar: {
                std::string str = row_view.GetStringUnsafe(col_idx);
                target_row_builder_.AppendString(str.data(), str.size());
                break;
            }
            default:
                continue;
        }
    }
    return true;
}

}  // namespace codec
}  // namespace fesql
