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


#include "codec/sdk_codec.h"

#include <string>
#include <utility>

#include "base/hash.h"
#include "codec/row_codec.h"

namespace rtidb {
namespace codec {

SDKCodec::SDKCodec(const ::rtidb::nameserver::TableInfo& table_info)
    : format_version_(table_info.format_version()),
      base_schema_size_(0),
      modify_times_(0),
      version_schema_(),
      last_ver_(1) {
    if (table_info.column_desc_v1_size() > 0) {
        ParseColumnDesc(table_info.column_desc_v1());
    } else {
        base_schema_size_ = table_info.column_desc_size();
        for (uint32_t idx = 0; idx < (uint32_t)table_info.column_desc_size(); idx++) {
            const auto& cur_column_desc = table_info.column_desc(idx);
            if (format_version_ == 1) {
                auto col = schema_.Add();
                col->set_name(cur_column_desc.name());
                auto iter = DATA_TYPE_MAP.find(cur_column_desc.type());
                if (iter != DATA_TYPE_MAP.end()) {
                    col->set_data_type(iter->second);
                }
            }
            schema_idx_map_.emplace(cur_column_desc.name(), idx);
            ::rtidb::codec::ColumnDesc column_desc;
            ::rtidb::codec::ColType type = SchemaCodec::ConvertType(cur_column_desc.type());
            column_desc.type = type;
            column_desc.name = cur_column_desc.name();
            column_desc.add_ts_idx = cur_column_desc.add_ts_idx();
            old_schema_.push_back(std::move(column_desc));
            if (cur_column_desc.add_ts_idx() && table_info.column_key_size() == 0) {
                auto col_key = index_.Add();
                col_key->set_index_name(cur_column_desc.name());
                col_key->add_col_name(cur_column_desc.name());
            }
        }
    }
    if (table_info.column_key_size() > 0) {
        index_.Clear();
        index_.CopyFrom(table_info.column_key());
    }
    const Schema& add_schema = table_info.added_column_desc();
    ParseSchemaVer(table_info.schema_versions(), add_schema);
    ParseAddedColumnDesc(add_schema);
    for (const auto& name : table_info.partition_key()) {
        auto iter = schema_idx_map_.find(name);
        if (iter != schema_idx_map_.end()) {
            partition_col_idx_.push_back(iter->second);
        }
    }
}

SDKCodec::SDKCodec(const ::rtidb::api::TableMeta& table_info)
    : format_version_(table_info.format_version()),
      base_schema_size_(0),
      modify_times_(0),
      last_ver_(1) {
    if (table_info.column_desc_size() > 0) {
        ParseColumnDesc(table_info.column_desc());
    } else if (!table_info.schema().empty()) {
        ::rtidb::codec::SchemaCodec scodec;
        scodec.Decode(table_info.schema(), old_schema_);
        for (uint32_t idx = 0; idx < old_schema_.size(); idx++) {
            schema_idx_map_.emplace(old_schema_[idx].name, idx);
            if (old_schema_[idx].add_ts_idx) {
                auto col_key = index_.Add();
                col_key->set_index_name(old_schema_[idx].name);
                col_key->add_col_name(old_schema_[idx].name);
            }
            if (old_schema_[idx].is_ts_col) {
                ts_idx_.push_back(idx);
            }
        }
        base_schema_size_ = old_schema_.size();
    }
    if (table_info.column_key_size() > 0) {
        index_.Clear();
        index_.CopyFrom(table_info.column_key());
    }
    const Schema& add_schema = table_info.added_column_desc();
    ParseSchemaVer(table_info.schema_versions(), add_schema);
    ParseAddedColumnDesc(table_info.added_column_desc());
}

void SDKCodec::ParseColumnDesc(const Schema& column_desc) {
    base_schema_size_ = column_desc.size();
    if (format_version_ == 1) {
        schema_.CopyFrom(column_desc);
    }
    for (uint32_t idx = 0; idx < (uint32_t)column_desc.size(); idx++) {
        const auto& cur_column_desc = column_desc.Get(idx);
        schema_idx_map_.emplace(cur_column_desc.name(), idx);
        if (cur_column_desc.is_ts_col()) {
            ts_idx_.push_back(idx);
        }
        if (format_version_ == 0) {
            ::rtidb::codec::ColumnDesc column_desc;
            ::rtidb::codec::ColType type = SchemaCodec::ConvertType(cur_column_desc.type());
            column_desc.type = type;
            column_desc.name = cur_column_desc.name();
            column_desc.add_ts_idx = cur_column_desc.add_ts_idx();
            column_desc.is_ts_col = cur_column_desc.is_ts_col();
            old_schema_.push_back(std::move(column_desc));
        }
        if (cur_column_desc.add_ts_idx()) {
            auto col_key = index_.Add();
            col_key->set_index_name(cur_column_desc.name());
            col_key->add_col_name(cur_column_desc.name());
        }
    }
}

void SDKCodec::ParseAddedColumnDesc(const Schema& column_desc) {
    if (format_version_ == 1) {
        uint32_t idx = schema_.size();
        for (const auto& col : column_desc) {
            rtidb::common::ColumnDesc* new_col = schema_.Add();
            new_col->CopyFrom(col);
            schema_idx_map_.emplace(col.name(), idx);
            idx++;
        }
        return;
    }
    uint32_t idx = old_schema_.size();
    for (const auto& cur_column_desc : column_desc) {
        schema_idx_map_.emplace(cur_column_desc.name(), idx);
        idx++;
        ::rtidb::codec::ColumnDesc column_desc;
        ::rtidb::codec::ColType type = SchemaCodec::ConvertType(cur_column_desc.type());
        column_desc.type = type;
        column_desc.name = cur_column_desc.name();
        column_desc.add_ts_idx = cur_column_desc.add_ts_idx();
        column_desc.is_ts_col = cur_column_desc.is_ts_col();
        old_schema_.push_back(std::move(column_desc));
    }
    modify_times_ = column_desc.size();
}

void SDKCodec::ParseSchemaVer(const VerSchema& ver_schema, const Schema& add_schema) {
    if (format_version_ != 1) {
        return;
    }
    std::shared_ptr<Schema> origin_schema = std::make_shared<Schema>(schema_);
    version_schema_.insert(std::make_pair(1, origin_schema));
    for (const auto pair : ver_schema) {
        int32_t ver = pair.id();
        int32_t times = pair.field_count();
        std::shared_ptr<Schema> base_schema = std::make_shared<Schema>(schema_);
        int remain_size = times - schema_.size();
        if (remain_size < 0 || remain_size > add_schema.size())  {
            continue;
        }
        for (int i = 0; i < remain_size; i++) {
            rtidb::common::ColumnDesc* col = base_schema->Add();
            col->CopyFrom(add_schema.Get(i));
        }
        version_schema_.insert(std::make_pair(ver, base_schema));
        last_ver_ = ver;
    }
}

int SDKCodec::EncodeDimension(
    const std::map<std::string, std::string>& raw_data, uint32_t pid_num,
    std::map<uint32_t, Dimension>* dimensions) {
    uint32_t dimension_idx = 0;
    for (const auto& column_key : index_) {
        if (column_key.flag() != 0) {
            dimension_idx++;
            continue;
        }
        std::string key;
        for (const auto& name : column_key.col_name()) {
            auto pos = raw_data.find(name);
            if (pos == raw_data.end()) {
                return -1;
            }
            if (!key.empty()) {
                key += "|";
            }
            key += pos->second;
        }
        if (key.empty()) {
            const std::string& index_name = column_key.index_name();
            auto pos = raw_data.find(index_name);
            if (pos == raw_data.end()) {
                return -1;
            }
            key = pos->second;
        }
        uint32_t pid = 0;
        if (pid_num > 0) {
            pid = (uint32_t)(::rtidb::base::hash64(key) % pid_num);
        }
        auto pair = dimensions->emplace(pid, Dimension());
        pair.first->second.emplace_back(std::move(key), dimension_idx);
        dimension_idx++;
    }
    return 0;
}

int SDKCodec::EncodeDimension(const std::vector<std::string>& raw_data,
                              uint32_t pid_num,
                              std::map<uint32_t, Dimension>* dimensions) {
    uint32_t dimension_idx = 0;
    for (const auto& column_key : index_) {
        if (column_key.flag() != 0) {
            dimension_idx++;
            continue;
        }
        std::string key;
        for (const auto& name : column_key.col_name()) {
            auto iter = schema_idx_map_.find(name);
            if (iter == schema_idx_map_.end() ||
                iter->second >= raw_data.size()) {
                return -1;
            }
            if (!key.empty()) {
                key += "|";
            }
            key += raw_data[iter->second];
        }
        if (key.empty()) {
            const std::string& name = column_key.index_name();
            auto iter = schema_idx_map_.find(name);
            if (iter == schema_idx_map_.end() ||
                iter->second >= raw_data.size()) {
                return -1;
            }
            key = raw_data[iter->second];
        }
        uint32_t pid = 0;
        if (pid_num > 0) {
            pid = (uint32_t)(::rtidb::base::hash64(key) % pid_num);
        }
        auto pair = dimensions->emplace(pid, Dimension());
        pair.first->second.emplace_back(std::move(key), dimension_idx);
        dimension_idx++;
    }
    return 0;
}
int SDKCodec::EncodeTsDimension(const std::vector<std::string>& raw_data,
                          std::vector<uint64_t>* ts_dimensions, uint64_t default_ts) {
    for (auto idx : ts_idx_) {
        if (idx >= raw_data.size()) {
            return -1;
        }
        if (rtidb::codec::NONETOKEN == raw_data[idx]) {
            ts_dimensions->push_back(default_ts);
            continue;
        }
        try {
            ts_dimensions->push_back(
                boost::lexical_cast<uint64_t>(raw_data[idx]));
        } catch (std::exception const& e) {
            ts_dimensions->push_back(default_ts);
        }
    }
    return 0;
}
int SDKCodec::EncodeTsDimension(const std::vector<std::string>& raw_data,
                                std::vector<uint64_t>* ts_dimensions) {
    for (auto idx : ts_idx_) {
        if (idx >= raw_data.size()) {
            return -1;
        }
        try {
            ts_dimensions->push_back(
                boost::lexical_cast<uint64_t>(raw_data[idx]));
        } catch (std::exception const& e) {
            return -1;
        }
    }
    return 0;
}

int SDKCodec::EncodeRow(const std::vector<std::string>& raw_data,
                        std::string* row) {
    if (format_version_ == 1) {
        auto ret = RowCodec::EncodeRow(raw_data, schema_, last_ver_, *row);
        return ret.code;
    } else {
        auto ret = RowCodec::EncodeRow(raw_data, old_schema_, modify_times_, row);
        return ret.code;
    }
}

int SDKCodec::DecodeRow(const std::string& row, std::vector<std::string>* value) {
    if (format_version_ == 1) {
        const int8_t* data = reinterpret_cast<const int8_t*>(row.data());
        int32_t ver = rtidb::codec::RowView::GetSchemaVersion(data);
        auto it = version_schema_.find(ver);
        if (it == version_schema_.end()) {
            return -1;
        }
        auto schema = it->second;
        if (!RowCodec::DecodeRow(*schema, data, schema->size(), false, 0, schema->size(), *value)) {
            return -1;
        }
    } else {
        rtidb::base::Slice data(row);
        if (!RowCodec::DecodeRow(base_schema_size_, base_schema_size_ + modify_times_, data, value)) {
            return -1;
        }
    }
    return 0;
}

std::vector<std::string> SDKCodec::GetColNames() {
    std::vector<std::string> cols;
    if (format_version_ == 1) {
        for (const auto& column_desc : schema_) {
            cols.push_back(column_desc.name());
        }
    } else {
        for (const auto& column_desc : old_schema_) {
            cols.push_back(column_desc.name);
        }
    }
    return cols;
}

int SDKCodec::CombinePartitionKey(const std::vector<std::string>& raw_data,
                                  std::string* key) {
    if (partition_col_idx_.empty()) {
        return -1;
    }
    key->clear();
    for (auto idx : partition_col_idx_) {
        if (idx >= raw_data.size()) {
            return -1;
        }
        if (!key->empty()) key->append("|");
        key->append(raw_data[idx]);
    }
    return 0;
}

}  // namespace codec
}  // namespace rtidb
