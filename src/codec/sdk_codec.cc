// sdk_codec.cc
// Copyright (C) 2017 4paradigm.com

#include "codec/sdk_codec.h"
#include <string>
#include <utility>
#include "base/hash.h"
#include "codec/row_codec.h"

namespace rtidb {
namespace codec {

SDKCodec::SDKCodec(const ::rtidb::nameserver::TableInfo& table_info)
    : modify_times_(0) {
    format_version_ = table_info.format_version();
    if (table_info.column_key_size() > 0) {
        index_.CopyFrom(table_info.column_key());
    }
    if (table_info.column_desc_v1_size() > 0) {
        if (format_version_ == 1) {
            schema_.CopyFrom(table_info.column_desc_v1());
        }
        for (uint32_t idx = 0; idx < (uint32_t)table_info.column_desc_v1_size();
             idx++) {
            const auto& cur_column_desc = table_info.column_desc_v1(idx);
            schema_idx_map_.emplace(cur_column_desc.name(), idx);
            if (cur_column_desc.is_ts_col()) {
                ts_idx_.push_back(idx);
            }
            if (format_version_ == 0) {
                ::rtidb::codec::ColumnDesc column_desc;
                ::rtidb::codec::ColType type =
                    SchemaCodec::ConvertType(cur_column_desc.type());
                column_desc.type = type;
                column_desc.name = cur_column_desc.name();
                column_desc.add_ts_idx = cur_column_desc.add_ts_idx();
                column_desc.is_ts_col = cur_column_desc.is_ts_col();
                old_schema_.push_back(std::move(column_desc));
            }
            if (cur_column_desc.add_ts_idx() &&
                table_info.column_key_size() == 0) {
                auto col_key = index_.Add();
                col_key->set_index_name(cur_column_desc.name());
                col_key->add_col_name(cur_column_desc.name());
            }
        }
    } else {
        for (uint32_t idx = 0; idx < (uint32_t)table_info.column_desc_size();
             idx++) {
            const auto& cur_column_desc = table_info.column_desc(idx);
            schema_idx_map_.emplace(cur_column_desc.name(), idx);
            ::rtidb::codec::ColumnDesc column_desc;
            ::rtidb::codec::ColType type =
                SchemaCodec::ConvertType(cur_column_desc.type());
            column_desc.type = type;
            column_desc.name = cur_column_desc.name();
            column_desc.add_ts_idx = cur_column_desc.add_ts_idx();
            old_schema_.push_back(std::move(column_desc));
            if (cur_column_desc.add_ts_idx() &&
                table_info.column_key_size() == 0) {
                auto col_key = index_.Add();
                col_key->set_index_name(cur_column_desc.name());
                col_key->add_col_name(cur_column_desc.name());
            }
        }
    }
    uint32_t idx = old_schema_.size();
    for (const auto& cur_column_desc : table_info.added_column_desc()) {
        schema_idx_map_.emplace(cur_column_desc.name(), idx);
        idx++;
        ::rtidb::codec::ColumnDesc column_desc;
        ::rtidb::codec::ColType type =
            SchemaCodec::ConvertType(cur_column_desc.type());
        column_desc.type = type;
        column_desc.name = cur_column_desc.name();
        column_desc.add_ts_idx = cur_column_desc.add_ts_idx();
        column_desc.is_ts_col = cur_column_desc.is_ts_col();
        old_schema_.push_back(std::move(column_desc));
    }
    modify_times_ = table_info.added_column_desc_size();
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
        auto pair = dimensions->insert(std::make_pair(pid, Dimension()));
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
        auto pair = dimensions->insert(std::make_pair(pid, Dimension()));
        pair.first->second.emplace_back(std::move(key), dimension_idx);
        dimension_idx++;
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
        auto ret = RowCodec::EncodeRow(raw_data, schema_, *row);
        return ret.code;
    } else {
        auto ret =
            RowCodec::EncodeRow(raw_data, old_schema_, modify_times_, row);
        return ret.code;
    }
}

}  // namespace codec
}  // namespace rtidb
