//
//// display.h
//// Copyright (C) 2019 4paradigm.com
//// Author denglong
//// Date 2019-05-10
////
#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <snappy.h>
#include "proto/tablet.pb.h"
#include "proto/client.pb.h"
#include "proto/name_server.pb.h"
#include "tprinter.h"
#include "base/schema_codec.h"
#include "storage/segment.h"
#include "proto/type.pb.h"

DECLARE_uint32(max_col_display_length);

namespace rtidb {
namespace base {

static const std::unordered_map<::rtidb::type::DataType, std::string> DATA_TYPE_STR_MAP = {
    {::rtidb::type::kBool, "bool"},
    {::rtidb::type::kSmallInt, "smallInt"},
    {::rtidb::type::kInt, "int"},
    {::rtidb::type::kBigInt, "bigInt"},
    {::rtidb::type::kFloat, "float"},
    {::rtidb::type::kDouble, "double"},
    {::rtidb::type::kTimestamp, "timestamp"},
    {::rtidb::type::kDate, "date"},
    {::rtidb::type::kVarchar, "varchar"},
    {::rtidb::type::kBlob, "blob"}
};

static std::string DataTypeToStr(::rtidb::type::DataType data_type) {
    const auto& iter = DATA_TYPE_STR_MAP.find(data_type);
    if (iter == DATA_TYPE_STR_MAP.end()) {
        return "-";
    } else {
        return iter->second;
    }
}

static void PrintSchema(const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>& column_desc_field, 
        ::rtidb::type::TableType table_type) {
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
        if (table_type == ::rtidb::type::kTimeSeries) {
            row.push_back(column_desc.type());
        } else {
            row.push_back(DataTypeToStr(column_desc.data_type())); 
        }
        tp.AddRow(row);
        idx++;
    }
    tp.Print(true);
}

static void PrintSchema(const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>& column_desc_field) {
    return PrintSchema(column_desc_field, ::rtidb::type::kTimeSeries);
}

static void PrintSchema(const ::rtidb::nameserver::TableInfo& table_info) {
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
            if (!table_info.has_table_type() || table_info.table_type() == ::rtidb::type::kTimeSeries) {
                row.push_back(column_desc.type());
            } else {
                row.push_back(DataTypeToStr(column_desc.data_type()));
            }
            tp.AddRow(row);
            idx++;
        }
    } else {
        for (const auto& column_desc : table_info.column_desc()) {
            row.clear();
            row.push_back(std::to_string(idx));
            row.push_back(column_desc.name());
            row.push_back(column_desc.type());
            column_desc.add_ts_idx() ? row.push_back("yes") : row.push_back("no");
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

static void PrintSchema(const google::protobuf::RepeatedPtrField<::rtidb::nameserver::ColumnDesc>& column_desc_field) {
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

static void PrintSchema(const std::string& schema, bool has_column_key) {
    std::vector<::rtidb::base::ColumnDesc> raw;
    ::rtidb::base::SchemaCodec codec;
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
            case ::rtidb::base::ColType::kInt32:
                row.push_back("int32");
                break;
            case ::rtidb::base::ColType::kInt64:
                row.push_back("int64");
                break;
            case ::rtidb::base::ColType::kUInt32:
                row.push_back("uint32");
                break;
            case ::rtidb::base::ColType::kUInt64:
                row.push_back("uint64");
                break;
            case ::rtidb::base::ColType::kDouble:
                row.push_back("double");
                break;
            case ::rtidb::base::ColType::kFloat:
                row.push_back("float");
                break;
            case ::rtidb::base::ColType::kString:
                row.push_back("string");
                break;
            case ::rtidb::base::ColType::kTimestamp:
                row.push_back("timestamp");
                break;
            case ::rtidb::base::ColType::kDate:
                row.push_back("date");
                break;
            case ::rtidb::base::ColType::kInt16:
                row.push_back("int16");
                break;
            case ::rtidb::base::ColType::kUInt16:
                row.push_back("uint16");
                break;
            case ::rtidb::base::ColType::kBool:
                row.push_back("bool");
                break;
            default:
                break;
        }
        if (!has_column_key) {
            if (raw[i].add_ts_idx) {
                row.push_back("yes");
            }else {
                row.push_back("no");
            }
        }
        tp.AddRow(row);
    }
    tp.Print(true);
}

static void PrintSchema(const std::string& schema) {
    return PrintSchema(schema, false);
}
static void PrintColumnKey(const ::rtidb::api::TTLType& ttl_type, const ::rtidb::storage::TTLDesc& ttl_desc,
        const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>& column_desc_field,
        const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>& column_key_field) {
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("index_name");
    row.push_back("col_name");
    row.push_back("ts_col");
    row.push_back("ttl");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    std::map<std::string, ::rtidb::storage::TTLDesc> ttl_map;
    for (const auto& column_desc : column_desc_field) {
        if (column_desc.is_ts_col()) {
            if (column_desc.has_abs_ttl() || column_desc.has_lat_ttl()) {
                ttl_map.insert(std::make_pair(column_desc.name(), ::rtidb::storage::TTLDesc(column_desc.abs_ttl(), column_desc.lat_ttl())));
            } else if (column_desc.has_ttl()) {
                if (ttl_type == ::rtidb::api::kAbsoluteTime) {
                    ttl_map.insert(std::make_pair(column_desc.name(), ::rtidb::storage::TTLDesc(column_desc.ttl(), 0)));
                } else {
                    ttl_map.insert(std::make_pair(column_desc.name(), ::rtidb::storage::TTLDesc(0, column_desc.ttl())));
                }
            } else {
                ttl_map.insert(std::make_pair(column_desc.name(), ttl_desc));
            }
        }
    }
    uint32_t idx = 0;
    if (column_key_field.size() > 0) {
        for (const auto& column_key : column_key_field) {
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
                        row_copy.push_back(ttl_map[ts_name].ToString(ttl_type));
                    } else {
                        row_copy.push_back(ttl_desc.ToString(ttl_type));
                    }
                    tp.AddRow(row_copy);
                    idx++;
                }
            } else {
                row.push_back("-");
                row.push_back(ttl_desc.ToString(ttl_type));
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
                    row.push_back(ttl_desc.ToString(ttl_type));
                } else {
                    auto iter = ttl_map.begin();
                    row.push_back(iter->first);
                    row.push_back(iter->second.ToString(ttl_type));
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
            double double_col = 0.0;
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

static void FillTableRow(uint32_t full_schema_size,
        const std::vector<::rtidb::base::ColumnDesc>& base_schema,
        const char* row,
        const uint32_t row_size,
        std::vector<std::string>& vrow) {
    rtidb::base::FlatArrayIterator fit(row, row_size, base_schema.size());
    while (full_schema_size > 0) {
        std::string col;
        if (!fit.Valid()) {
            full_schema_size--;
            vrow.push_back("");
            continue;
        } else if (fit.GetType() == ::rtidb::base::ColType::kString) {
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
            double double_col = 0.0;
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
        full_schema_size--;
        fit.Next();
        vrow.push_back(col);
    }
}

static void ShowTableRows(const std::vector<ColumnDesc>& base_columns,
        const std::vector<ColumnDesc>& raw,
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
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    uint32_t index = 1;
    while (it->Valid()) {
        std::vector<std::string> vrow;
        vrow.push_back(boost::lexical_cast<std::string>(index));
        if (!has_ts_col) {
            vrow.push_back(boost::lexical_cast<std::string>(it->GetKey()));
        }
        const char* str = NULL;
        uint32_t str_size = 0;
        if (compress_type == ::rtidb::nameserver::kSnappy) {
            std::string uncompressed;
            ::snappy::Uncompress(it->GetValue().data(), it->GetValue().size(), &uncompressed);
            str = uncompressed.c_str();
            str_size = uncompressed.size();
        } else {
            str = it->GetValue().data();
            str_size = it->GetValue().size();
        }
        if (base_columns.size() == 0) {
            ::rtidb::base::FillTableRow(raw, str, str_size, vrow);
        } else {
            ::rtidb::base::FillTableRow(raw.size(), base_columns, str, str_size, vrow);
        }
        tp.AddRow(vrow);
        index ++;
        it->Next();
    }
    tp.Print(true);
}

static void ShowTableRows(const std::vector<ColumnDesc>& raw,
                   ::rtidb::base::KvIterator* it,
                   const ::rtidb::nameserver::CompressType compress_type) {
    std::vector<ColumnDesc> base_columns;
    return ShowTableRows(base_columns, raw, it, compress_type);
}

static void ShowTableRows(const std::string& key, ::rtidb::base::KvIterator* it,
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
    row.push_back("diskused");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    int32_t row_width = row.size();
    for (const auto& value : tables) {
        if (value.table_partition_size() == 0) {
            row.clear();
            row.push_back(value.name());
            row.push_back(std::to_string(value.tid()));
            for(int i = row.size(); i < row_width; i++) {
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
                for(int i = row.size(); i < row_width; i++) {
                    row.push_back("-");
                }
                tp.AddRow(row);
                continue;
            }
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
                if (value.has_ttl_desc()) {
                    if (value.ttl_desc().ttl_type() == ::rtidb::api::TTLType::kLatestTime) {
                        row.push_back(std::to_string(value.ttl_desc().lat_ttl()));
                    } else if (value.ttl_desc().ttl_type() == ::rtidb::api::TTLType::kAbsAndLat) {
                        row.push_back(std::to_string(value.ttl_desc().abs_ttl()) + "min&&" + std::to_string(value.ttl_desc().lat_ttl()));
                    } else if (value.ttl_desc().ttl_type() == ::rtidb::api::TTLType::kAbsOrLat) {
                        row.push_back(std::to_string(value.ttl_desc().abs_ttl()) + "min||" + std::to_string(value.ttl_desc().lat_ttl()));
                    } else {
                        row.push_back(std::to_string(value.ttl_desc().abs_ttl()) + "min");
                    }
                } else {
                    if (value.ttl_type() == "kLatestTime") {
                        row.push_back(std::to_string(value.ttl()));
                    } else {
                        row.push_back(std::to_string(value.ttl()) + "min");
                    }
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
                if (value.table_partition(idx).partition_meta(meta_idx).has_record_byte_size() &&
                        (!value.has_storage_mode() || value.storage_mode() == ::rtidb::common::StorageMode::kMemory)) {
                    row.push_back(::rtidb::base::HumanReadableString(value.table_partition(idx).partition_meta(meta_idx).record_byte_size()));
                } else {
                    row.push_back("-");
                }
                row.push_back(::rtidb::base::HumanReadableString(value.table_partition(idx).partition_meta(meta_idx).diskused()));
                tp.AddRow(row);
            }
        }
    }
    tp.Print(true);
}

static void PrintTableStatus(const std::vector<::rtidb::api::TableStatus>& status_vec) {
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
            if (table_status.ttl_desc().ttl_type() == ::rtidb::api::TTLType::kLatestTime) {
                row.push_back(std::to_string(table_status.ttl_desc().lat_ttl()));
            } else if (table_status.ttl_desc().ttl_type() == ::rtidb::api::TTLType::kAbsAndLat) {
                row.push_back(std::to_string(table_status.ttl_desc().abs_ttl()) + "min&&" + std::to_string(table_status.ttl_desc().lat_ttl()));
            } else if (table_status.ttl_desc().ttl_type() == ::rtidb::api::TTLType::kAbsOrLat) {
                row.push_back(std::to_string(table_status.ttl_desc().abs_ttl()) + "min||" + std::to_string(table_status.ttl_desc().lat_ttl()));
            } else {
                row.push_back(std::to_string(table_status.ttl_desc().abs_ttl()) + "min");
            }
        } else {
            if (table_status.ttl_type() == ::rtidb::api::TTLType::kLatestTime) {
                row.push_back(std::to_string(table_status.ttl()));
            } else {
                row.push_back(std::to_string(table_status.ttl()) + "min");
            }
        }
        row.push_back(std::to_string(table_status.time_offset()) + "s");
        if (!table_status.has_storage_mode() || table_status.storage_mode() == ::rtidb::common::StorageMode::kMemory) {
            row.push_back(::rtidb::base::HumanReadableString(table_status.record_byte_size() + table_status.record_idx_byte_size()));
        } else {
            row.push_back("-");
        }
        row.push_back(::rtidb::api::CompressType_Name(table_status.compress_type()));
        if (!table_status.has_storage_mode() || table_status.storage_mode() == ::rtidb::common::StorageMode::kMemory) {
            row.push_back(std::to_string(table_status.skiplist_height()));
            row.push_back("kMemory");
        } else {
            row.push_back("-");
            row.push_back(::rtidb::common::StorageMode_Name(table_status.storage_mode()));
        }
        tp.AddRow(row);
    }
    tp.Print(true);
}

static void PrintTableInformation(std::vector<::rtidb::nameserver::TableInfo>& tables) {
    if (tables.size() < 1) {
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
    } else if (table.ttl_type() == "kLatestTime"){
        ttl_type = ::rtidb::api::kLatestTime;
        abs_ttl = 0;
        lat_ttl = table.ttl();
    }
    ::rtidb::storage::TTLDesc ttl_desc(abs_ttl, lat_ttl);
    std::string name = table.name();
    std::string replica_num = std::to_string(table.replica_num());
    std::string partition_num = std::to_string(table.partition_num());
    std::string compress_type = ::rtidb::nameserver::CompressType_Name(table.compress_type());
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
    row.push_back(ttl_desc.ToString(ttl_type));
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
    tp.Print(true);
}

}
}
