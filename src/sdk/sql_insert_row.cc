/*
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

#include "sdk/sql_insert_row.h"

#include <stdint.h>

#include <string>

#include "glog/logging.h"

namespace openmldb {
namespace sdk {

SQLInsertRows::SQLInsertRows(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                             std::shared_ptr<hybridse::sdk::Schema> schema, DefaultValueMap default_map,
                             uint32_t default_str_length)
    : table_info_(table_info), schema_(schema), default_map_(default_map), default_str_length_(default_str_length) {}

std::shared_ptr<SQLInsertRow> SQLInsertRows::NewRow() {
    if (!rows_.empty() && !rows_.back()->IsComplete()) {
        return std::shared_ptr<SQLInsertRow>();
    }
    std::shared_ptr<SQLInsertRow> row =
        std::make_shared<SQLInsertRow>(table_info_, schema_, default_map_, default_str_length_);
    rows_.push_back(row);
    return row;
}

SQLInsertRow::SQLInsertRow(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                           std::shared_ptr<hybridse::sdk::Schema> schema, DefaultValueMap default_map,
                           uint32_t default_string_length)
    : table_info_(table_info),
      schema_(schema),
      default_map_(default_map),
      default_string_length_(default_string_length),
      rb_(table_info->column_desc()),
      val_(),
      str_size_(0) {
    std::map<std::string, uint32_t> column_name_map;
    for (int idx = 0; idx < table_info_->column_desc_size(); idx++) {
        column_name_map.emplace(table_info_->column_desc(idx).name(), idx);
    }
    if (table_info_->column_key_size() > 0) {
        index_map_.clear();
        raw_dimensions_.clear();
        for (int idx = 0; idx < table_info_->column_key_size(); ++idx) {
            for (const auto& column : table_info_->column_key(idx).col_name()) {
                index_map_[idx].push_back(column_name_map[column]);
                raw_dimensions_[column_name_map[column]] = hybridse::codec::NONETOKEN;
            }
        }
    }
}

bool SQLInsertRow::Init(int str_length) {
    str_size_ = str_length + default_string_length_;
    uint32_t row_size = rb_.CalTotalLength(str_size_);
    val_.resize(row_size);
    int8_t* buf = reinterpret_cast<int8_t*>(&(val_[0]));
    bool ok = rb_.SetBuffer(reinterpret_cast<int8_t*>(buf), row_size);
    if (!ok) {
        return false;
    }
    MakeDefault();
    return true;
}

void SQLInsertRow::PackDimension(const std::string& val) { raw_dimensions_[rb_.GetAppendPos()] = val; }



const std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& SQLInsertRow::GetDimensions() {
    if (!dimensions_.empty()) {
        return dimensions_;
    }
    uint32_t pid_num = table_info_->table_partition_size();
    uint32_t pid = 0;
    for (const auto& kv : index_map_) {
        std::string key;
        for (uint32_t idx : kv.second) {
            if (!key.empty()) {
                key += "|";
            }
            key += raw_dimensions_[idx];
        }
        if (pid_num > 0) {
            pid = (uint32_t)(::openmldb::base::hash64(key) % pid_num);
        }
        auto iter = dimensions_.find(pid);
        if (iter == dimensions_.end()) {
            auto result = dimensions_.emplace(pid, std::vector<std::pair<std::string, uint32_t>>());
            iter = result.first;
        }
        iter->second.push_back(std::make_pair(key, kv.first));
    }
    return dimensions_;
}

bool SQLInsertRow::MakeDefault() {
    auto it = default_map_->find(rb_.GetAppendPos());
    if (it != default_map_->end()) {
        if (it->second->IsNull()) {
            return AppendNULL();
        }
        switch (table_info_->column_desc(rb_.GetAppendPos()).data_type()) {
            case openmldb::type::kBool:
                return AppendBool(it->second->GetInt());
            case openmldb::type::kSmallInt:
                return AppendInt16(it->second->GetSmallInt());
            case openmldb::type::kInt:
                return AppendInt32(it->second->GetInt());
            case openmldb::type::kBigInt:
                return AppendInt64(it->second->GetLong());
            case openmldb::type::kFloat:
                return AppendFloat(it->second->GetFloat());
            case openmldb::type::kDouble:
                return AppendDouble(it->second->GetDouble());
            case openmldb::type::kDate:
                return AppendDate(it->second->GetInt());
            case openmldb::type::kTimestamp:
                return AppendTimestamp(it->second->GetLong());
            case openmldb::type::kVarchar:
            case openmldb::type::kString:
                return AppendString(it->second->GetStr());
            default:
                return false;
        }
    }
    return true;
}

bool SQLInsertRow::AppendBool(bool val) {
    if (IsDimension()) {
        PackDimension(val ? "true" : "false");
    }
    if (rb_.AppendBool(val)) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendInt16(int16_t val) {
    if (IsDimension()) {
        PackDimension(std::to_string(val));
    }
    if (rb_.AppendInt16(val)) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendInt32(int32_t val) {
    if (IsDimension()) {
        PackDimension(std::to_string(val));
    }
    if (rb_.AppendInt32(val)) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendInt64(int64_t val) {
    if (IsDimension()) {
        PackDimension(std::to_string(val));
    }
    if (rb_.AppendInt64(val)) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendTimestamp(int64_t val) {
    if (IsDimension()) {
        PackDimension(std::to_string(val));
    }
    if (rb_.AppendTimestamp(val)) {
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
    if (IsDimension()) {
        if (val.empty()) {
            PackDimension(hybridse::codec::EMPTY_STRING);
        } else {
            PackDimension(val);
        }
    }
    str_size_ -= val.size();
    if (rb_.AppendString(val.c_str(), val.size())) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendString(const char* string_buffer_var_name, uint32_t length) {
    if (IsDimension()) {
        if (0 == length) {
            PackDimension(hybridse::codec::EMPTY_STRING);
        } else {
            PackDimension(std::string(string_buffer_var_name, length));
        }
    }
    str_size_ -= length;
    if (rb_.AppendString(string_buffer_var_name, length)) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendDate(uint32_t year, uint32_t month, uint32_t day) {
    if (IsDimension()) {
        if (year < 1900 || year > 9999) return false;
        if (month < 1 || month > 12) return false;
        if (day < 1 || day > 31) return false;
        int32_t date = (year - 1900) << 16;
        date = date | ((month - 1) << 8);
        date = date | day;
        PackDimension(std::to_string(date));
    }
    if (rb_.AppendDate(year, month, day)) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendDate(int32_t date) {
    if (IsDimension()) {
        PackDimension(std::to_string(date));
    }
    if (rb_.AppendDate(date)) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::AppendNULL() {
    if (IsDimension()) {
        PackDimension(hybridse::codec::NONETOKEN);
    }
    if (rb_.AppendNULL()) {
        return MakeDefault();
    }
    return false;
}

bool SQLInsertRow::IsComplete() { return rb_.IsComplete(); }

bool SQLInsertRow::Build() { return str_size_ == 0; }

}  // namespace sdk
}  // namespace openmldb
