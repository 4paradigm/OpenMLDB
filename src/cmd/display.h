//
// display.h
// Copyright (C) 2019 4paradigm.com
// Author denglong
// Date 2019-05-10
//
#pragma once

#include <gflags/gflags.h>
#include <snappy.h>
#include <tprinter.h>

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
#include "proto/client.pb.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "storage/segment.h"

DECLARE_uint32(max_col_display_length);

namespace rtidb {
namespace cmd {

static void TransferString(std::vector<std::string>* vec) {
    std::for_each(vec->begin(), vec->end(), [](std::string& str) {
        if (str == ::rtidb::codec::NONETOKEN) {
            str = "-";
        } else if (str == ::rtidb::codec::EMPTY_STRING) {
            str = "";
        }
    });
}

__attribute__((unused)) static void PrintSchema(
    const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>&
        column_desc_field) {
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("name");
    row.push_back("type");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    uint32_t idx = 0;
    for (const auto& column_desc : column_desc_field) {
        row.clear();
        row.push_back(std::to_string(idx));
        row.push_back(column_desc.name());
        row.push_back(column_desc.type());
        tp.AddRow(row);
        idx++;
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintSchema(
    const ::rtidb::nameserver::TableInfo& table_info) {
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("name");
    row.push_back("type");
    if (table_info.column_desc_v1_size() == 0) {
        row.push_back("index");
    }
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    uint32_t idx = 0;
    if (table_info.column_desc_v1_size() > 0) {
        for (const auto& column_desc : table_info.column_desc_v1()) {
            row.clear();
            row.push_back(std::to_string(idx));
            row.push_back(column_desc.name());
            row.push_back(column_desc.type());
            tp.AddRow(row);
            idx++;
        }
    } else {
        for (const auto& column_desc : table_info.column_desc()) {
            row.clear();
            row.push_back(std::to_string(idx));
            row.push_back(column_desc.name());
            row.push_back(column_desc.type());
            column_desc.add_ts_idx() ? row.push_back("yes")
                                     : row.push_back("no");
            tp.AddRow(row);
            idx++;
        }
    }
    for (const auto& column_desc : table_info.added_column_desc()) {
        row.clear();
        row.push_back(std::to_string(idx));
        row.push_back(column_desc.name());
        row.push_back(column_desc.type());
        if (table_info.column_desc_v1_size() == 0) {
            row.push_back("no");
        }
        tp.AddRow(row);
        idx++;
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintSchema(
    const google::protobuf::RepeatedPtrField<::rtidb::nameserver::ColumnDesc>&
        column_desc_field) {
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("name");
    row.push_back("type");
    row.push_back("index");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    uint32_t idx = 0;
    for (const auto& column_desc : column_desc_field) {
        row.clear();
        row.push_back(std::to_string(idx));
        row.push_back(column_desc.name());
        row.push_back(column_desc.type());
        column_desc.add_ts_idx() ? row.push_back("yes") : row.push_back("no");
        tp.AddRow(row);
        idx++;
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintSchema(const std::string& schema,
                                                bool has_column_key) {
    std::vector<::rtidb::codec::ColumnDesc> raw;
    ::rtidb::codec::SchemaCodec codec;
    codec.Decode(schema, raw);
    std::vector<std::string> header;
    header.push_back("#");
    header.push_back("name");
    header.push_back("type");
    if (!has_column_key) {
        header.push_back("index");
    }
    ::baidu::common::TPrinter tp(header.size());
    tp.AddRow(header);
    for (uint32_t i = 0; i < raw.size(); i++) {
        std::vector<std::string> row;
        row.push_back(boost::lexical_cast<std::string>(i));
        row.push_back(raw[i].name);
        switch (raw[i].type) {
            case ::rtidb::codec::ColType::kInt32:
                row.push_back("int32");
                break;
            case ::rtidb::codec::ColType::kInt64:
                row.push_back("int64");
                break;
            case ::rtidb::codec::ColType::kUInt32:
                row.push_back("uint32");
                break;
            case ::rtidb::codec::ColType::kUInt64:
                row.push_back("uint64");
                break;
            case ::rtidb::codec::ColType::kDouble:
                row.push_back("double");
                break;
            case ::rtidb::codec::ColType::kFloat:
                row.push_back("float");
                break;
            case ::rtidb::codec::ColType::kString:
                row.push_back("string");
                break;
            case ::rtidb::codec::ColType::kTimestamp:
                row.push_back("timestamp");
                break;
            case ::rtidb::codec::ColType::kDate:
                row.push_back("date");
                break;
            case ::rtidb::codec::ColType::kInt16:
                row.push_back("int16");
                break;
            case ::rtidb::codec::ColType::kUInt16:
                row.push_back("uint16");
                break;
            case ::rtidb::codec::ColType::kBool:
                row.push_back("bool");
                break;
            default:
                break;
        }
        if (!has_column_key) {
            if (raw[i].add_ts_idx) {
                row.push_back("yes");
            } else {
                row.push_back("no");
            }
        }
        tp.AddRow(row);
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintSchema(const std::string& schema) {
    return PrintSchema(schema, false);
}

__attribute__((unused)) static void PrintColumnKey(
    const ::rtidb::storage::TTLSt& ttl_st,
    const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>&
        column_desc_field,
    const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>&
        column_key_field) {
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("index_name");
    row.push_back("col_name");
    row.push_back("ts_col");
    row.push_back("ttl");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    std::map<std::string, ::rtidb::storage::TTLSt> ttl_map;
    for (const auto& column_desc : column_desc_field) {
        if (column_desc.is_ts_col()) {
            if (column_desc.has_abs_ttl() || column_desc.has_lat_ttl()) {
                ttl_map.emplace(column_desc.name(),
                    ::rtidb::storage::TTLSt(column_desc.abs_ttl(), column_desc.lat_ttl(), ttl_st.ttl_type));
            } else if (column_desc.has_ttl()) {
                if (ttl_st.ttl_type == ::rtidb::storage::kAbsoluteTime) {
                    ttl_map.emplace(column_desc.name(),
                            ::rtidb::storage::TTLSt(column_desc.ttl(), 0, ttl_st.ttl_type));
                } else {
                    ttl_map.emplace(column_desc.name(),
                        ::rtidb::storage::TTLSt(0, column_desc.ttl(), ttl_st.ttl_type));
                }
            } else {
                ttl_map.emplace(column_desc.name(), ttl_st);
            }
        }
    }
    uint32_t idx = 0;
    if (column_key_field.size() > 0) {
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
            if (column_key.ts_name_size() > 0) {
                for (const auto& ts_name : column_key.ts_name()) {
                    std::vector<std::string> row_copy = row;
                    row_copy[0] = std::to_string(idx);
                    row_copy.push_back(ts_name);
                    if (column_key.has_ttl()) {
                        ::rtidb::storage::TTLSt cur_ttl_st(column_key.ttl());
                        cur_ttl_st.abs_ttl = cur_ttl_st.abs_ttl / (60 * 1000);
                        row_copy.push_back(cur_ttl_st.ToString());
                    } else {
                        if (ttl_map.find(ts_name) != ttl_map.end()) {
                            row_copy.push_back(ttl_map[ts_name].ToString());
                        } else {
                            row_copy.push_back(ttl_st.ToString());
                        }
                    }
                    tp.AddRow(row_copy);
                    idx++;
                }
            } else {
                row.push_back("-");
                if (column_key.has_ttl()) {
                    ::rtidb::storage::TTLSt cur_ttl_st(column_key.ttl());
                    cur_ttl_st.abs_ttl = cur_ttl_st.abs_ttl / (60 * 1000);
                    row.push_back(cur_ttl_st.ToString());
                } else {
                    row.push_back(ttl_st.ToString());
                }
                tp.AddRow(row);
                idx++;
            }
        }
    } else {
        for (const auto& column_desc : column_desc_field) {
            if (column_desc.add_ts_idx()) {
                row.clear();
                row.push_back(std::to_string(idx));
                row.push_back(column_desc.name());
                row.push_back(column_desc.name());
                if (ttl_map.empty()) {
                    row.push_back("-");
                    row.push_back(ttl_st.ToString());
                } else {
                    auto iter = ttl_map.begin();
                    row.push_back(iter->first);
                    row.push_back(iter->second.ToString());
                }
                tp.AddRow(row);
                idx++;
            }
        }
    }
    tp.Print(true);
}

__attribute__((unused)) static void ShowTableRows(
    bool is_compress, ::rtidb::codec::SDKCodec* codec,
    ::rtidb::cmd::SDKIterator* it) {
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
        rtidb::base::Slice data = it->GetValue();
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
        vrow.insert(vrow.begin(), boost::lexical_cast<std::string>(index));
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
    const ::rtidb::api::TableMeta& table_info, ::rtidb::cmd::SDKIterator* it) {
    ::rtidb::codec::SDKCodec codec(table_info);
    bool is_compress =
        table_info.compress_type() == ::rtidb::api::CompressType::kSnappy
            ? true
            : false;
    ShowTableRows(is_compress, &codec, it);
}

__attribute__((unused)) static void ShowTableRows(
    const ::rtidb::nameserver::TableInfo& table_info,
    ::rtidb::cmd::SDKIterator* it) {
    ::rtidb::codec::SDKCodec codec(table_info);
    bool is_compress = table_info.compress_type() == ::rtidb::nameserver::kSnappy ? true : false;
    ShowTableRows(is_compress, &codec, it);
}

__attribute__((unused)) static void ShowTableRows(
    const std::string& key, ::rtidb::cmd::SDKIterator* it,
    const ::rtidb::nameserver::CompressType compress_type) {
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
        if (compress_type == ::rtidb::nameserver::kSnappy) {
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
    const std::vector<::rtidb::nameserver::TableInfo>& tables) {
    std::vector<std::string> row;
    row.push_back("name");
    row.push_back("tid");
    row.push_back("pid");
    row.push_back("endpoint");
    row.push_back("role");
    row.push_back("ttl");
    row.push_back("is_alive");
    row.push_back("compress_type");
    row.push_back("offset");
    row.push_back("record_cnt");
    row.push_back("memused");
    row.push_back("diskused");
    ::baidu::common::TPrinter tp(row.size());
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
                if (value.has_ttl_desc()) {
                    if (value.ttl_desc().ttl_type() ==
                        ::rtidb::api::TTLType::kLatestTime) {
                        row.push_back(
                            std::to_string(value.ttl_desc().lat_ttl()));
                    } else if (value.ttl_desc().ttl_type() ==
                               ::rtidb::api::TTLType::kAbsAndLat) {
                        row.push_back(
                            std::to_string(value.ttl_desc().abs_ttl()) +
                            "min&&" +
                            std::to_string(value.ttl_desc().lat_ttl()));
                    } else if (value.ttl_desc().ttl_type() ==
                               ::rtidb::api::TTLType::kAbsOrLat) {
                        row.push_back(
                            std::to_string(value.ttl_desc().abs_ttl()) +
                            "min||" +
                            std::to_string(value.ttl_desc().lat_ttl()));
                    } else {
                        row.push_back(
                            std::to_string(value.ttl_desc().abs_ttl()) + "min");
                    }
                } else {
                    if (value.ttl_type() == "kLatestTime") {
                        row.push_back(std::to_string(value.ttl()));
                    } else {
                        row.push_back(std::to_string(value.ttl()) + "min");
                    }
                }
                if (value.table_partition(idx)
                        .partition_meta(meta_idx)
                        .is_alive()) {
                    row.push_back("yes");
                } else {
                    row.push_back("no");
                }
                if (value.has_compress_type()) {
                    row.push_back(::rtidb::nameserver::CompressType_Name(
                        value.compress_type()));
                } else {
                    row.push_back("kNoCompress");
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
                        .has_record_byte_size() &&
                    (!value.has_storage_mode() ||
                     value.storage_mode() ==
                         ::rtidb::common::StorageMode::kMemory)) {
                    row.push_back(::rtidb::base::HumanReadableString(
                        value.table_partition(idx)
                            .partition_meta(meta_idx)
                            .record_byte_size()));
                } else {
                    row.push_back("-");
                }
                row.push_back(::rtidb::base::HumanReadableString(
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
    const std::vector<::rtidb::api::TableStatus>& status_vec) {
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
    row.push_back("storage_mode");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);

    for (const auto& table_status : status_vec) {
        std::vector<std::string> row;
        row.push_back(std::to_string(table_status.tid()));
        row.push_back(std::to_string(table_status.pid()));
        row.push_back(std::to_string(table_status.offset()));
        row.push_back(::rtidb::api::TableMode_Name(table_status.mode()));
        row.push_back(::rtidb::api::TableState_Name(table_status.state()));
        if (table_status.is_expire()) {
            row.push_back("true");
        } else {
            row.push_back("false");
        }
        if (table_status.has_ttl_desc()) {
            if (table_status.ttl_desc().ttl_type() ==
                ::rtidb::api::TTLType::kLatestTime) {
                row.push_back(
                    std::to_string(table_status.ttl_desc().lat_ttl()));
            } else if (table_status.ttl_desc().ttl_type() ==
                       ::rtidb::api::TTLType::kAbsAndLat) {
                row.push_back(
                    std::to_string(table_status.ttl_desc().abs_ttl()) +
                    "min&&" +
                    std::to_string(table_status.ttl_desc().lat_ttl()));
            } else if (table_status.ttl_desc().ttl_type() ==
                       ::rtidb::api::TTLType::kAbsOrLat) {
                row.push_back(
                    std::to_string(table_status.ttl_desc().abs_ttl()) +
                    "min||" +
                    std::to_string(table_status.ttl_desc().lat_ttl()));
            } else {
                row.push_back(
                    std::to_string(table_status.ttl_desc().abs_ttl()) + "min");
            }
        } else {
            if (table_status.ttl_type() == ::rtidb::api::TTLType::kLatestTime) {
                row.push_back(std::to_string(table_status.ttl()));
            } else {
                row.push_back(std::to_string(table_status.ttl()) + "min");
            }
        }
        row.push_back(std::to_string(table_status.time_offset()) + "s");
        if (!table_status.has_storage_mode() ||
            table_status.storage_mode() ==
                ::rtidb::common::StorageMode::kMemory) {
            row.push_back(::rtidb::base::HumanReadableString(
                table_status.record_byte_size() +
                table_status.record_idx_byte_size()));
        } else {
            row.push_back("-");
        }
        row.push_back(
            ::rtidb::api::CompressType_Name(table_status.compress_type()));
        if (!table_status.has_storage_mode() ||
            table_status.storage_mode() ==
                ::rtidb::common::StorageMode::kMemory) {
            row.push_back(std::to_string(table_status.skiplist_height()));
            row.push_back("kMemory");
        } else {
            row.push_back("-");
            row.push_back(
                ::rtidb::common::StorageMode_Name(table_status.storage_mode()));
        }
        tp.AddRow(row);
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintTableInformation(
    std::vector<::rtidb::nameserver::TableInfo>& tables) {  // NOLINT
    if (tables.empty()) {
        return;
    }
    ::rtidb::nameserver::TableInfo table = tables[0];
    std::vector<std::string> row;
    row.push_back("attribute");
    row.push_back("value");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    uint64_t abs_ttl = table.ttl();
    uint64_t lat_ttl = 0;
    ::rtidb::api::TTLType ttl_type = ::rtidb::api::kAbsoluteTime;
    if (table.has_ttl_desc()) {
        ttl_type = table.ttl_desc().ttl_type();
        abs_ttl = table.ttl_desc().abs_ttl();
        lat_ttl = table.ttl_desc().lat_ttl();
    } else if (table.ttl_type() == "kLatestTime") {
        ttl_type = ::rtidb::api::kLatestTime;
        abs_ttl = 0;
        lat_ttl = table.ttl();
    }
    ::rtidb::storage::TTLSt ttl_st(abs_ttl, lat_ttl, ::rtidb::storage::TTLSt::ConvertTTLType(ttl_type));
    std::string name = table.name();
    std::string replica_num = std::to_string(table.replica_num());
    std::string partition_num = std::to_string(table.partition_num());
    std::string compress_type =
        ::rtidb::nameserver::CompressType_Name(table.compress_type());
    std::string storage_mode = "kMemory";
    if (table.has_storage_mode()) {
        storage_mode = ::rtidb::common::StorageMode_Name(table.storage_mode());
    }
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
    row.push_back("ttl");
    row.push_back(ttl_st.ToString());
    tp.AddRow(row);
    row.clear();
    row.push_back("ttl_type");
    row.push_back(::rtidb::api::TTLType_Name(ttl_type));
    tp.AddRow(row);
    row.clear();
    row.push_back("compress_type");
    row.push_back(compress_type);
    tp.AddRow(row);
    row.clear();
    row.push_back("storage_mode");
    row.push_back(storage_mode);
    tp.AddRow(row);
    row.clear();
    row.push_back("record_cnt");
    row.push_back(std::to_string(record_cnt));
    tp.AddRow(row);
    row.clear();
    row.push_back("memused");
    row.push_back(::rtidb::base::HumanReadableString(memused));
    tp.AddRow(row);
    row.clear();
    row.push_back("diskused");
    row.push_back(::rtidb::base::HumanReadableString(diskused));
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
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    for (auto db : dbs) {
        row.clear();
        row.push_back(db);
        tp.AddRow(row);
    }
    tp.Print(true);
}

}  // namespace cmd
}  // namespace rtidb
