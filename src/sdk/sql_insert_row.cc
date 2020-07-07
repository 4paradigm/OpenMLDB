/*
 * sql_insert_row.cc
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

#include "sdk/sql_insert_row.h"

#include <stdint.h>

#include <string>
#include <unordered_map>

#include "base/fe_strings.h"
#include "glog/logging.h"

namespace rtidb {
namespace sdk {

#define BitMapSize(size) (((size) >> 3) + !!((size)&0x07))
static constexpr uint8_t SDK_VERSION_LENGTH = 2;
static constexpr uint8_t SDK_SIZE_LENGTH = 4;
static constexpr uint8_t SDK_HEADER_LENGTH =
    SDK_VERSION_LENGTH + SDK_SIZE_LENGTH;
static constexpr uint32_t SDK_UINT24_MAX = (1 << 24) - 1;
static const std::unordered_map<::fesql::sdk::DataType, uint8_t>
    SDK_TYPE_SIZE_MAP = {{::fesql::sdk::kTypeBool, sizeof(bool)},
                         {::fesql::sdk::kTypeInt16, sizeof(int16_t)},
                         {::fesql::sdk::kTypeInt32, sizeof(int32_t)},
                         {::fesql::sdk::kTypeDate, sizeof(int32_t)},
                         {::fesql::sdk::kTypeFloat, sizeof(float)},
                         {::fesql::sdk::kTypeInt64, sizeof(int64_t)},
                         {::fesql::sdk::kTypeTimestamp, sizeof(int64_t)},
                         {::fesql::sdk::kTypeDouble, sizeof(double)}};

static inline uint8_t SDKGetAddrLength(uint32_t size) {
    if (size <= UINT8_MAX) {
        return 1;
    } else if (size <= UINT16_MAX) {
        return 2;
    } else if (size <= SDK_UINT24_MAX) {
        return 3;
    } else {
        return 4;
    }
}

inline uint32_t SDKGetStartOffset(int32_t column_count) {
    return SDK_HEADER_LENGTH + BitMapSize(column_count);
}

SQLInsertRows::SQLInsertRows(
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
    std::shared_ptr<std::map<uint32_t, ::fesql::node::ConstNode*>> default_map,
    uint32_t default_str_length)
    : table_info_(table_info),
      default_map_(default_map),
      default_str_length_(default_str_length) {}

std::shared_ptr<SQLInsertRow> SQLInsertRows::NewRow() {
    if (!rows_.empty() && !rows_.back()->IsComplete()) {
        return std::shared_ptr<SQLInsertRow>();
    }
    std::shared_ptr<SQLInsertRow> row = std::make_shared<SQLInsertRow>(
        table_info_, default_map_, default_str_length_);
    rows_.push_back(row);
    return row;
}

SQLInsertRow::SQLInsertRow(
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
    std::shared_ptr<std::map<uint32_t, ::fesql::node::ConstNode*>> default_map,
    uint32_t default_string_length)
    : table_info_(table_info),
      default_map_(default_map),
      default_string_length_(default_string_length),
      rb_(table_info->column_desc_v1()),
      val_(),
      buf_(NULL) {
    for (int idx = 0; idx < table_info_->column_desc_v1_size(); idx++) {
        auto type = ConvertType(table_info_->column_desc_v1(idx).data_type());
        if (table_info_->column_desc_v1(idx).is_ts_col()) {
            ts_set_.insert(idx);
        } else if (table_info_->column_desc_v1(idx).add_ts_idx()) {
            index_set_.insert(idx);
        }
    }
}

bool SQLInsertRow::Init(int str_length) {
    uint32_t row_size = rb_.CalTotalLength(str_length + default_string_length_);
    val_.resize(row_size);
    buf_ = reinterpret_cast<int8_t*>(&(val_[0]));
    bool ok = rb_.SetBuffer(reinterpret_cast<int8_t*>(buf_), row_size);
    if (!ok) {
        return false;
    }
    MakeDefault();
    return true;
}

bool SQLInsertRow::GetIndex(const std::string& val) {
    auto index_it = index_set_.find(rb_.GetCnt());
    if (index_it != index_set_.end()) {
        dimensions_.push_back(std::make_pair(val, rb_.GetCnt()));
        return true;
    }
    return false;
}

bool SQLInsertRow::GetTs(uint64_t ts) {
    auto ts_it = ts_set_.find(rb_.GetCnt());
    if (ts_it != ts_set_.end()) {
        ts_.push_back(ts);
        return true;
    }
    return false;
}

bool SQLInsertRow::MakeDefault() {
    auto it = default_map_->find(rb_.GetCnt());
    if (it != default_map_->end()) {
        switch (it->second->GetDataType()) {
            case ::fesql::node::kBool:
                if (it->second->GetInt()) {
                    return AppendBool(true);
                } else {
                    return AppendBool(false);
                }
            case ::fesql::node::kInt16:
                return AppendInt16(it->second->GetSmallInt());
            case ::fesql::node::kInt32:
                return AppendInt32(it->second->GetInt());
            case ::fesql::node::kInt64:
                return AppendInt64(it->second->GetAsInt64());
            case ::fesql::node::kFloat:
                return AppendFloat(static_cast<float>(it->second->GetDouble()));
            case ::fesql::node::kDouble:
                return AppendDouble(it->second->GetDouble());
            case ::fesql::node::kTimestamp:
                return AppendInt64(it->second->GetAsInt64());
            case ::fesql::node::kVarchar:
                return AppendString(it->second->GetStr());
            case ::fesql::node::kDate: {
                int32_t year;
                int32_t month;
                int32_t day;
                if (!it->second->GetAsDate(&year, &month, &day)) {
                    return false;
                } else {
                    return AppendDate(year, month, day);
                }
                break;
            }
            default:
                return false;
        }
    }
    return true;
}

bool SQLInsertRow::AppendBool(bool val) {
    if (rb_.AppendBool(val)) {
        GetIndex(val ? "true" : "false");
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendInt16(int16_t val) {
    if (rb_.AppendInt16(val)) {
        GetIndex(std::to_string(val));
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendInt32(int32_t val) {
    if (rb_.AppendInt32(val)) {
        GetIndex(std::to_string(val));
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendInt64(int64_t val) {
    if (rb_.AppendInt64(val)) {
        GetIndex(std::to_string(val));
        GetTs(val);
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendTimestamp(int64_t val) {
    if (rb_.AppendTimestamp(val)) {
        GetIndex(std::to_string(val));
        GetTs(val);
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendFloat(float val) {
    if (rb_.AppendFloat(val)) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendDouble(double val) {
    if (rb_.AppendDouble(val)) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendString(const std::string& val) {
    if (rb_.AppendString(val.c_str(), val.size())) {
        GetIndex(val);
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendString(const char* val, uint32_t length) {
    if (rb_.AppendString(val, length)) {
        GetIndex(std::string(val, length));
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendDate(uint32_t year, uint32_t month, uint32_t day) {
    if (rb_.AppendDate(year, month, day)) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendNULL() {
    // todo: deal with null
    if (rb_.AppendNULL()) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::Build() { return rb_.Build(); }

bool SQLInsertRow::IsComplete() { return rb_.IsComplete(); }

}  // namespace sdk
}  // namespace rtidb
