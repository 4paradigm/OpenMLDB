//
//// display.h
//// Copyright (C) 2019 4paradigm.com
//// Author denglong
//// Date 2019-05-10
////
#pragma once

#include <string>
#include <vector>
#include "proto/tablet.pb.h"
#include "proto/client.pb.h"
#include "proto/name_server.pb.h"
#include "tprinter.h"
#include "base/schema_codec.h"

namespace rtidb {
namespace base {

static void PrintSchema(const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>& column_desc_field) {
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

static void PrintSchema(const google::protobuf::RepeatedPtrField<::rtidb::nameserver::ColumnDesc>& column_desc_field) {
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("name");
    row.push_back("type");
    row.push_back("index");
    row.push_back("ts_col");
    row.push_back("ttl");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    uint32_t idx = 0;
    for (const auto& column_desc : column_desc_field) {
        row.clear();
        row.push_back(std::to_string(idx));
        row.push_back(column_desc.name());
        row.push_back(column_desc.type());
        column_desc.add_ts_idx() ? row.push_back("yes") : row.push_back("no");
        row.push_back("-");
        row.push_back("-");
        row.push_back("-");
        tp.AddRow(row);
        idx++;
    }
    tp.Print(true);
}

static void PrintColumnKey(const ::rtidb::nameserver::TableInfo& table_info) {
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("index_name");
    row.push_back("col_name");
    row.push_back("ts_col");
    row.push_back("ttl");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    uint64_t ttl = table_info.ttl();
    std::map<std::string, uint64_t> ttl_map;
    for (const auto& column_desc :  table_info.column_desc_v1()) {
        if (column_desc.is_ts_col()) {
            if (column_desc.has_ttl()) {
                ttl_map.insert(std::make_pair(column_desc.name(), column_desc.ttl()));
            } else {
                ttl_map.insert(std::make_pair(column_desc.name(), ttl));
            }
        }
    }
    std::string ttl_suff = table_info.ttl_type() == "kLatestTime" ? "" : "min";
    uint32_t idx = 0;
    if (table_info.column_key_size() > 0) {
        for (const auto& column_key : table_info.column_key()) {
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
                    if (ttl_map.find(ts_name) != ttl_map.end()) {
                        row_copy.push_back(std::to_string(ttl_map[ts_name]) + ttl_suff);
                    } else {
                        row_copy.push_back(std::to_string(ttl) + ttl_suff);
                    }
                    tp.AddRow(row_copy);
                    idx++;
                }
            } else {
                row.push_back("-");
                row.push_back(std::to_string(ttl) + ttl_suff);
                tp.AddRow(row);
                idx++;
            }
        }
    } else {
        for (const auto& column_desc :  table_info.column_desc_v1()) {
            if (column_desc.add_ts_idx()) {
                row.clear();
                row.push_back(std::to_string(idx));
                row.push_back(column_desc.name());
                row.push_back(column_desc.name());
                if (ttl_map.empty()) {
                    row.push_back("-");
                    row.push_back(std::to_string(ttl) + ttl_suff);
                } else {
                    auto iter = ttl_map.begin();
                    row.push_back(iter->first);
                    row.push_back(std::to_string(iter->second) + ttl_suff);
                }
                tp.AddRow(row);
                idx++;
            }
        }
    }
    tp.Print(true);
}

static void FillTableRow(const std::vector<::rtidb::base::ColumnDesc>& schema, 
                  const char* row,
                  const uint32_t row_size,
                  std::vector<std::string>& vrow) {
    rtidb::base::FlatArrayIterator fit(row, row_size, schema.size());
    while (fit.Valid()) {
        std::string col;
        if (fit.GetType() == ::rtidb::base::ColType::kString) {
            fit.GetString(&col);
        } else if (fit.GetType() == ::rtidb::base::ColType::kUInt16) {
            uint16_t uint16_col = 0;
            fit.GetUInt16(&uint16_col);
            col = boost::lexical_cast<std::string>(uint16_col);
        } else if (fit.GetType() == ::rtidb::base::ColType::kInt16) {
            int16_t int16_col = 0;
            fit.GetInt16(&int16_col);
            col = boost::lexical_cast<std::string>(int16_col);
        } else if (fit.GetType() == ::rtidb::base::ColType::kInt32) {
            int32_t int32_col = 0;
            fit.GetInt32(&int32_col);
            col = boost::lexical_cast<std::string>(int32_col);
        } else if (fit.GetType() == ::rtidb::base::ColType::kInt64) {
            int64_t int64_col = 0;
            fit.GetInt64(&int64_col);
            col = boost::lexical_cast<std::string>(int64_col);
        } else if (fit.GetType() == ::rtidb::base::ColType::kUInt32) {
            uint32_t uint32_col = 0;
            fit.GetUInt32(&uint32_col);
            col = boost::lexical_cast<std::string>(uint32_col);
        } else if (fit.GetType() == ::rtidb::base::ColType::kUInt64) {
            uint64_t uint64_col = 0;
            fit.GetUInt64(&uint64_col);
            col = boost::lexical_cast<std::string>(uint64_col);
        } else if (fit.GetType() == ::rtidb::base::ColType::kDouble) {
            double double_col = 0.0d;
            fit.GetDouble(&double_col);
            col = boost::lexical_cast<std::string>(double_col);
        } else if (fit.GetType() == ::rtidb::base::ColType::kFloat) {
            float float_col = 0.0f;
            fit.GetFloat(&float_col);
            col = boost::lexical_cast<std::string>(float_col);
        } else if(fit.GetType() == ::rtidb::base::ColType::kTimestamp) {
            uint64_t ts = 0;
            fit.GetTimestamp(&ts);
            col = boost::lexical_cast<std::string>(ts);
        } else if(fit.GetType() == ::rtidb::base::ColType::kDate) {
            uint64_t dt = 0;
            fit.GetDate(&dt);
            time_t rawtime = (time_t)dt / 1000;
            tm* timeinfo = localtime(&rawtime);
            char buf[20];
            strftime(buf, 20, "%Y-%m-%d", timeinfo);
            col.assign(buf);
        } else if(fit.GetType() == ::rtidb::base::ColType::kBool) {
            bool value = false;
            fit.GetBool(&value);
            if (value) {
                col = "true";
            } else {
                col = "false";
            }
        }
        fit.Next();
        vrow.push_back(col);
    }
}

static void ShowTableRows(const std::vector<ColumnDesc>& raw, 
                   ::rtidb::base::KvIterator* it, 
                   const ::rtidb::nameserver::CompressType compress_type) {
    bool has_ts_col = SchemaCodec::HasTSCol(raw);
    std::vector<std::string> row;
    row.push_back("#");
    if (!has_ts_col) {
        row.push_back("ts");
    }
    for (uint32_t i = 0; i < raw.size(); i++) {
        row.push_back(raw[i].name);
    }
    ::baidu::common::TPrinter tp(row.size(), 128);
    tp.AddRow(row);
    uint32_t index = 1;
    while (it->Valid()) {
        std::vector<std::string> vrow;
        vrow.push_back(boost::lexical_cast<std::string>(index));
        if (!has_ts_col) {
            vrow.push_back(boost::lexical_cast<std::string>(it->GetKey()));
        }
        if (compress_type == ::rtidb::nameserver::kSnappy) {
            std::string uncompressed;
            ::snappy::Uncompress(it->GetValue().data(), it->GetValue().size(), &uncompressed);
            FillTableRow(raw, uncompressed.c_str(), uncompressed.length(), vrow); 
        } else {
            FillTableRow(raw, it->GetValue().data(), it->GetValue().size(), vrow); 
        }
        tp.AddRow(vrow);
        index ++;
        it->Next();
    }
    tp.Print(true);
}

static void ShowTableRows(const std::string& key, ::rtidb::base::KvIterator* it, 
                const ::rtidb::nameserver::CompressType compress_type) {
    ::baidu::common::TPrinter tp(4, 128);
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

static void PrintTableInfo(const std::vector<::rtidb::nameserver::TableInfo>& tables) {
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
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    for (const auto& value : tables) {
        for (int idx = 0; idx < value.table_partition_size(); idx++) {
            for (int meta_idx = 0; meta_idx < value.table_partition(idx).partition_meta_size(); meta_idx++) {
                row.clear();
                row.push_back(value.name());
                row.push_back(std::to_string(value.tid()));
                row.push_back(std::to_string(value.table_partition(idx).pid()));
                row.push_back(value.table_partition(idx).partition_meta(meta_idx).endpoint());
                if (value.table_partition(idx).partition_meta(meta_idx).is_leader()) {
                    row.push_back("leader");
                } else {
                    row.push_back("follower");
                }
                if (value.ttl_type() == "kLatestTime") {
                    row.push_back(std::to_string(value.ttl()));
                } else {
                    row.push_back(std::to_string(value.ttl()) + "min");
                }
                if (value.table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    row.push_back("yes");
                } else {
                    row.push_back("no");
                }
                if (value.has_compress_type()) {
                    row.push_back(::rtidb::nameserver::CompressType_Name(value.compress_type()));
                } else {
                    row.push_back("kNoCompress");
                }
                if (value.table_partition(idx).partition_meta(meta_idx).has_offset()) {
                    row.push_back(std::to_string(value.table_partition(idx).partition_meta(meta_idx).offset()));
                } else {
                    row.push_back("-");
                }
                if (value.table_partition(idx).partition_meta(meta_idx).has_record_cnt()) {
                    row.push_back(std::to_string(value.table_partition(idx).partition_meta(meta_idx).record_cnt()));
                } else {
                    row.push_back("-");
                }
                if (value.table_partition(idx).partition_meta(meta_idx).has_record_byte_size()) {
                    row.push_back(::rtidb::base::HumanReadableString(value.table_partition(idx).partition_meta(meta_idx).record_byte_size()));
                } else {
                    row.push_back("-");
                }
                tp.AddRow(row);
            }
        }
    }
    tp.Print(true);
}

}
}
