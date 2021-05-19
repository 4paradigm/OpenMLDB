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

#pragma once

#include <gflags/gflags.h>
#include <snappy.h>

#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cmd/sdk_iterator.h"
#include "codec/flat_array.h"
#include "codec/row_codec.h"
#include "codec/schema_codec.h"
#include "codec/sdk_codec.h"
#include "common/tprinter.h"
#include "proto/client.pb.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "storage/segment.h"

DECLARE_uint32(max_col_display_length);

namespace fedb {
namespace cmd {

static void TransferString(std::vector<std::string>* vec) {
    std::for_each(vec->begin(), vec->end(), [](std::string& str) {
        if (str == ::fedb::codec::NONETOKEN) {
            str = "-";
        } else if (str == ::fedb::codec::EMPTY_STRING) {
            str = "";
        }
    });
}

__attribute__((unused)) static void PrintSchema(
    const google::protobuf::RepeatedPtrField<::fedb::common::ColumnDesc>& column_desc_field) {
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("name");
    row.push_back("type");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    uint32_t idx = 0;
    for (const auto& column_desc : column_desc_field) {
        row.clear();
        row.push_back(std::to_string(idx));
        row.push_back(column_desc.name());
        auto iter = ::fedb::codec::DATA_TYPE_STR_MAP.find(column_desc.data_type());
        if (iter != ::fedb::codec::DATA_TYPE_STR_MAP.end()) {
            row.push_back(iter->second);
        } else {
            row.push_back("-");
        }
        tp.AddRow(row);
        idx++;
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintSchema(
    const google::protobuf::RepeatedPtrField<::fedb::common::ColumnDesc>& column_desc,
    const google::protobuf::RepeatedPtrField<::fedb::common::ColumnDesc>& added_column_desc) {
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("name");
    row.push_back("type");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    uint32_t idx = 0;
    for (const auto& column_desc : column_desc) {
        row.clear();
        row.push_back(std::to_string(idx));
        row.push_back(column_desc.name());
        auto iter = ::fedb::codec::DATA_TYPE_STR_MAP.find(column_desc.data_type());
        if (iter != ::fedb::codec::DATA_TYPE_STR_MAP.end()) {
            row.push_back(iter->second);
        } else {
            row.push_back("-");
        }
        tp.AddRow(row);
        idx++;
    }
    for (const auto& column_desc : added_column_desc) {
        row.clear();
        row.push_back(std::to_string(idx));
        row.push_back(column_desc.name());
        auto iter = ::fedb::codec::DATA_TYPE_STR_MAP.find(column_desc.data_type());
        if (iter != ::fedb::codec::DATA_TYPE_STR_MAP.end()) {
            row.push_back(iter->second);
        } else {
            row.push_back("-");
        }
        tp.AddRow(row);
        idx++;
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintColumnKey(
        const google::protobuf::RepeatedPtrField<::fedb::common::ColumnKey>& column_key_field) {
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("index_name");
    row.push_back("col_name");
    row.push_back("ts_col");
    row.push_back("ttl");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    uint32_t idx = 0;
    for (const auto& column_key : column_key_field) {
        if (column_key.flag() == 1) {
            continue;
        }
        row.clear();
        row.push_back(std::to_string(idx));
        row.push_back(column_key.index_name());
        std::string key;
        for (const auto& name : column_key.col_name()) {
            if (key.empty()) {
                key = name;
            } else {
                key += "|" + name;
            }
        }
        if (key.empty()) {
            key = column_key.index_name();
        }
        row.push_back(key);
        if (!column_key.ts_name().empty()) {
            row.push_back(column_key.ts_name());
        } else {
            row.push_back("-");
        }
        if (column_key.has_ttl()) {
            ::fedb::storage::TTLSt cur_ttl_st(column_key.ttl());
            cur_ttl_st.abs_ttl = cur_ttl_st.abs_ttl / (60 * 1000);
            row.push_back(cur_ttl_st.ToString());
        } else {
            row.push_back("-");
        }
        tp.AddRow(row);
        idx++;
    }
    tp.Print(true);
}

__attribute__((unused)) static void ShowTableRows(
    bool is_compress, ::fedb::codec::SDKCodec* codec,
    ::fedb::cmd::SDKIterator* it) {
    std::vector<std::string> row = codec->GetColNames();
    if (!codec->HasTSCol()) {
        row.insert(row.begin(), "ts");
    }
    row.insert(row.begin(), "#");
    uint64_t max_size = row.size();
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    uint32_t index = 1;
    while (it->Valid()) {
        std::vector<std::string> vrow;
        fedb::base::Slice data = it->GetValue();
        std::string value;
        if (is_compress) {
            ::snappy::Uncompress(data.data(), data.size(), &value);
        } else {
            value.assign(data.data(), data.size());
        }
        codec->DecodeRow(value, &vrow);
        if (!codec->HasTSCol()) {
            vrow.insert(vrow.begin(), std::to_string(it->GetKey()));
        }
        vrow.insert(vrow.begin(), std::to_string(index));
        TransferString(&vrow);
        uint64_t remain_size = max_size - vrow.size();
        for (uint64_t i = 0; i < remain_size; i++) {
            vrow.push_back("");
        }
        tp.AddRow(vrow);
        index++;
        it->Next();
    }
    tp.Print(true);
}

__attribute__((unused)) static void ShowTableRows(
    const ::fedb::api::TableMeta& table_info, ::fedb::cmd::SDKIterator* it) {
    ::fedb::codec::SDKCodec codec(table_info);
    bool is_compress =
        table_info.compress_type() == ::fedb::type::CompressType::kSnappy
            ? true
            : false;
    ShowTableRows(is_compress, &codec, it);
}

__attribute__((unused)) static void ShowTableRows(
    const ::fedb::nameserver::TableInfo& table_info,
    ::fedb::cmd::SDKIterator* it) {
    ::fedb::codec::SDKCodec codec(table_info);
    bool is_compress = table_info.compress_type() == ::fedb::type::CompressType::kSnappy ? true : false;
    ShowTableRows(is_compress, &codec, it);
}

__attribute__((unused)) static void ShowTableRows(
    const std::string& key, ::fedb::cmd::SDKIterator* it,
    const ::fedb::type::CompressType compress_type) {
    ::baidu::common::TPrinter tp(4, FLAGS_max_col_display_length);
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("key");
    row.push_back("ts");
    row.push_back("data");
    tp.AddRow(row);
    uint32_t index = 1;
    while (it->Valid()) {
        std::string value = it->GetValue().ToString();
        if (compress_type == ::fedb::type::CompressType::kSnappy) {
            std::string uncompressed;
            ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
            value = uncompressed;
        }
        row.clear();
        row.push_back(std::to_string(index));
        row.push_back(key);
        row.push_back(std::to_string(it->GetKey()));
        row.push_back(value);
        tp.AddRow(row);
        index++;
        it->Next();
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintTableInfo(
    const std::vector<::fedb::nameserver::TableInfo>& tables) {
    std::vector<std::string> row;
    row.push_back("name");
    row.push_back("tid");
    row.push_back("pid");
    row.push_back("endpoint");
    row.push_back("role");
    row.push_back("is_alive");
    row.push_back("offset");
    row.push_back("record_cnt");
    row.push_back("memused");
    row.push_back("diskused");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    int32_t row_width = row.size();
    for (const auto& value : tables) {
        if (value.table_partition_size() == 0) {
            row.clear();
            row.push_back(value.name());
            row.push_back(std::to_string(value.tid()));
            for (int i = row.size(); i < row_width; i++) {
                row.push_back("-");
            }
            tp.AddRow(row);
            continue;
        }
        for (int idx = 0; idx < value.table_partition_size(); idx++) {
            if (value.table_partition(idx).partition_meta_size() == 0) {
                row.clear();
                row.push_back(value.name());
                row.push_back(std::to_string(value.tid()));
                row.push_back(std::to_string(value.table_partition(idx).pid()));
                for (int i = row.size(); i < row_width; i++) {
                    row.push_back("-");
                }
                tp.AddRow(row);
                continue;
            }
            for (int meta_idx = 0;
                 meta_idx < value.table_partition(idx).partition_meta_size();
                 meta_idx++) {
                row.clear();
                row.push_back(value.name());
                row.push_back(std::to_string(value.tid()));
                row.push_back(std::to_string(value.table_partition(idx).pid()));
                row.push_back(value.table_partition(idx)
                                  .partition_meta(meta_idx)
                                  .endpoint());
                if (value.table_partition(idx)
                        .partition_meta(meta_idx)
                        .is_leader()) {
                    row.push_back("leader");
                } else {
                    row.push_back("follower");
                }
                if (value.table_partition(idx)
                        .partition_meta(meta_idx)
                        .is_alive()) {
                    row.push_back("yes");
                } else {
                    row.push_back("no");
                }
                if (value.table_partition(idx)
                        .partition_meta(meta_idx)
                        .has_offset()) {
                    row.push_back(std::to_string(value.table_partition(idx)
                                                     .partition_meta(meta_idx)
                                                     .offset()));
                } else {
                    row.push_back("-");
                }
                if (value.table_partition(idx)
                        .partition_meta(meta_idx)
                        .has_record_cnt()) {
                    row.push_back(std::to_string(value.table_partition(idx)
                                                     .partition_meta(meta_idx)
                                                     .record_cnt()));
                } else {
                    row.push_back("-");
                }
                if (value.table_partition(idx)
                        .partition_meta(meta_idx)
                        .has_record_byte_size()) {
                    row.push_back(::fedb::base::HumanReadableString(
                        value.table_partition(idx)
                            .partition_meta(meta_idx)
                            .record_byte_size()));
                } else {
                    row.push_back("-");
                }
                row.push_back(::fedb::base::HumanReadableString(
                    value.table_partition(idx)
                        .partition_meta(meta_idx)
                        .diskused()));
                tp.AddRow(row);
            }
        }
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintTableStatus(
    const std::vector<::fedb::api::TableStatus>& status_vec) {
    std::vector<std::string> row;
    row.push_back("tid");
    row.push_back("pid");
    row.push_back("offset");
    row.push_back("mode");
    row.push_back("state");
    row.push_back("enable_expire");
    row.push_back("ttl");
    row.push_back("ttl_offset");
    row.push_back("memused");
    row.push_back("compress_type");
    row.push_back("skiplist_height");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);

    for (const auto& table_status : status_vec) {
        std::vector<std::string> row;
        row.push_back(std::to_string(table_status.tid()));
        row.push_back(std::to_string(table_status.pid()));
        row.push_back(std::to_string(table_status.offset()));
        row.push_back(::fedb::api::TableMode_Name(table_status.mode()));
        row.push_back(::fedb::api::TableState_Name(table_status.state()));
        if (table_status.is_expire()) {
            row.push_back("true");
        } else {
            row.push_back("false");
        }
        row.push_back("0min");
        row.push_back(std::to_string(table_status.time_offset()) + "s");
        row.push_back(::fedb::base::HumanReadableString(
                    table_status.record_byte_size() +
                    table_status.record_idx_byte_size()));
        row.push_back(
            ::fedb::type::CompressType_Name(table_status.compress_type()));
        row.push_back(std::to_string(table_status.skiplist_height()));
        tp.AddRow(row);
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintTableInformation(
    std::vector<::fedb::nameserver::TableInfo>& tables) {  // NOLINT
    if (tables.empty()) {
        return;
    }
    ::fedb::nameserver::TableInfo table = tables[0];
    std::vector<std::string> row;
    row.push_back("attribute");
    row.push_back("value");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    ::fedb::common::TTLSt ttl_st;
    std::string name = table.name();
    std::string replica_num = std::to_string(table.replica_num());
    std::string partition_num = std::to_string(table.partition_num());
    std::string compress_type =
        ::fedb::type::CompressType_Name(table.compress_type());
    uint64_t record_cnt = 0;
    uint64_t memused = 0;
    uint64_t diskused = 0;
    for (int idx = 0; idx < table.table_partition_size(); idx++) {
        record_cnt += table.table_partition(idx).record_cnt();
        memused += table.table_partition(idx).record_byte_size();
        diskused += table.table_partition(idx).diskused();
    }
    row.clear();
    row.push_back("name");
    row.push_back(name);
    tp.AddRow(row);
    row.clear();
    row.push_back("replica_num");
    row.push_back(replica_num);
    tp.AddRow(row);
    row.clear();
    row.push_back("partition_num");
    row.push_back(partition_num);
    tp.AddRow(row);
    row.clear();
    row.push_back("compress_type");
    row.push_back(compress_type);
    tp.AddRow(row);
    row.clear();
    row.push_back("record_cnt");
    row.push_back(std::to_string(record_cnt));
    tp.AddRow(row);
    row.clear();
    row.push_back("memused");
    row.push_back(::fedb::base::HumanReadableString(memused));
    tp.AddRow(row);
    row.clear();
    row.push_back("diskused");
    row.push_back(::fedb::base::HumanReadableString(diskused));
    tp.AddRow(row);
    row.clear();
    row.push_back("format_version");
    row.push_back(std::to_string(table.format_version()));
    tp.AddRow(row);
    row.clear();
    row.push_back("partition_key");
    if (table.partition_key_size() > 0) {
        std::string partition_key;
        for (int idx = 0; idx < table.partition_key_size(); idx++) {
            if (idx > 0) partition_key.append(", ");
            partition_key.append(table.partition_key(idx));
        }
        row.push_back(std::move(partition_key));
    } else {
        row.push_back("-");
    }
    tp.AddRow(row);
    tp.Print(true);
}

__attribute__((unused)) static void PrintDatabase(
    const std::vector<std::string>& dbs) {
    std::vector<std::string> row;
    row.push_back("Databases");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    for (auto db : dbs) {
        row.clear();
        row.push_back(db);
        tp.AddRow(row);
    }
    tp.Print(true);
}

}  // namespace cmd
}  // namespace fedb
