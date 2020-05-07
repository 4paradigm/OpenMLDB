
// row_codec.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2020-04-30
//
#pragma once

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "codec/flat_array.h"
#include "codec/row_codec.h"
#include "codec/schema_codec.h"

namespace rtidb {
namespace codec {

class RowCodec {
 public:
    static int32_t CalStrLength(
        const std::map<std::string, std::string>& str_map, const Schema& schema,
        ::rtidb::base::ResultMsg& rm) {  // NOLINT
        int32_t str_len = 0;
        for (int i = 0; i < schema.size(); i++) {
            const ::rtidb::common::ColumnDesc& col = schema.Get(i);
            if (col.data_type() == ::rtidb::type::kVarchar ||
                col.data_type() == ::rtidb::type::kString) {
                auto iter = str_map.find(col.name());
                if (iter == str_map.end()) {
                    rm.code = -1;
                    rm.msg = col.name() + " not in str_map";
                    return -1;
                }
                if (!col.not_null() &&
                    (iter->second == "null" || iter->second == NONETOKEN)) {
                    continue;
                } else if (iter->second == "null" ||
                           iter->second == NONETOKEN) {
                    rm.code = -1;
                    rm.msg = col.name() + " should not be null";
                    return -1;
                }
                str_len += iter->second.length();
            }
        }
        return str_len;
    }

    static ::rtidb::base::ResultMsg EncodeRow(
        const std::map<std::string, std::string>& str_map, const Schema& schema,
        std::string& row) {  // NOLINT
        ::rtidb::base::ResultMsg rm;
        if (str_map.size() == 0 || schema.size() == 0 ||
            str_map.size() - schema.size() != 0) {
            rm.code = -1;
            rm.msg = "input error";
            return rm;
        }
        int32_t str_len = CalStrLength(str_map, schema, rm);
        if (str_len < 0) {
            return rm;
        }
        ::rtidb::codec::RowBuilder builder(schema);
        uint32_t size = builder.CalTotalLength(str_len);
        row.resize(size);
        builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
        for (int i = 0; i < schema.size(); i++) {
            const ::rtidb::common::ColumnDesc& col = schema.Get(i);
            auto iter = str_map.find(col.name());
            if (iter == str_map.end()) {
                rm.code = -1;
                rm.msg = col.name() + " not in str_map";
                return rm;
            }
            if (!col.not_null() &&
                (iter->second == "null" || iter->second == NONETOKEN)) {
                builder.AppendNULL();
                continue;
            } else if (iter->second == "null" || iter->second == NONETOKEN) {
                rm.code = -1;
                rm.msg = col.name() + " should not be null";
                return rm;
            }
            bool ok = false;
            try {
                switch (col.data_type()) {
                    case rtidb::type::kString:
                    case rtidb::type::kVarchar:
                        ok = builder.AppendString(iter->second.c_str(),
                                                  iter->second.length());
                        break;
                    case rtidb::type::kBool:
                        if (iter->second == "true") {
                            ok = builder.AppendBool(
                                    boost::lexical_cast<bool>(1));
                        } else if (iter->second == "false") {
                            ok = builder.AppendBool(
                                    boost::lexical_cast<bool>(0));
                        } else {
                            rm.code = -1;
                            rm.msg = "bool input format error";
                            return rm;
                        }
                        break;
                    case rtidb::type::kSmallInt:
                        ok = builder.AppendInt16(
                            boost::lexical_cast<uint16_t>(iter->second));
                        break;
                    case rtidb::type::kInt:
                        ok = builder.AppendInt32(
                            boost::lexical_cast<uint32_t>(iter->second));
                        break;
                    case rtidb::type::kBigInt:
                        ok = builder.AppendInt64(
                            boost::lexical_cast<uint64_t>(iter->second));
                        break;
                    case rtidb::type::kTimestamp:
                        ok = builder.AppendTimestamp(
                            boost::lexical_cast<uint64_t>(iter->second));
                        break;
                    case rtidb::type::kFloat:
                        ok = builder.AppendFloat(
                            boost::lexical_cast<float>(iter->second));
                        break;
                    case rtidb::type::kDouble:
                        ok = builder.AppendDouble(
                            boost::lexical_cast<double>(iter->second));
                        break;
                    case rtidb::type::kDate: {
                        std::vector<std::string> parts;
                        ::rtidb::base::SplitString(iter->second, "-", parts);
                        if (parts.size() != 3) {
                            rm.code = -1;
                            rm.msg = "bad data format " + iter->second;
                            return rm;
                        }
                        uint32_t year = boost::lexical_cast<uint32_t>(parts[0]);
                        uint32_t mon = boost::lexical_cast<uint32_t>(parts[1]);
                        uint32_t day = boost::lexical_cast<uint32_t>(parts[2]);
                        ok = builder.AppendDate(year, mon, day);
                        break;
                    }
                    default:
                        rm.code = -1;
                        rm.msg = "unsupported data type";
                        return rm;
                }
                if (!ok) {
                    rm.code = -1;
                    rm.msg = "append " +
                             ::rtidb::type::DataType_Name(col.data_type()) +
                             " error";
                    return rm;
                }
            } catch (std::exception const& e) {
                rm.code = -1;
                rm.msg = "input format error";
                return rm;
            }
        }
        rm.code = 0;
        rm.msg = "ok";
        return rm;
    }

    static bool DecodeRow(const Schema& schema,  // NOLINT
                          const ::rtidb::base::Slice& value,
                          std::vector<std::string>& value_vec) {  // NOLINT
        rtidb::codec::RowView rv(
            schema, reinterpret_cast<int8_t*>(const_cast<char*>(value.data())),
            value.size());
        return DecodeRow(schema, rv, 0, schema.size(), &value_vec);
    }

    static bool DecodeRow(const Schema& schema,  // NOLINT
                          const ::rtidb::base::Slice& value,
                          int start, int length,
                          std::vector<std::string>& value_vec) {  // NOLINT
        rtidb::codec::RowView rv(
            schema, reinterpret_cast<int8_t*>(const_cast<char*>(value.data())),
            value.size());
        return DecodeRow(schema, rv, start, length, &value_vec);
    }

    static bool DecodeRow(const Schema& schema,                   // NOLINT
                          rtidb::codec::RowView& rv,              // NOLINT
                          std::vector<std::string>& value_vec) {  // NOLINT
        return DecodeRow(schema, rv, 0, schema.size(), &value_vec);
    }

    static bool DecodeRow(const Schema& schema,
                          rtidb::codec::RowView& rv,  // NOLINT
                          int start, int length,
                          std::vector<std::string>* value_vec) {
        int end = start + length;
        if (length <= 0 || end > schema.size()) {
            return false;
        }
        for (int32_t i = 0; i < end; i++) {
            if (rv.IsNULL(i)) {
                value_vec->emplace_back(NONETOKEN);
                continue;
            }
            std::string col;
            auto type = schema.Get(i).data_type();
            if (type == rtidb::type::kInt) {
                int32_t val;
                int ret = rv.GetInt32(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kTimestamp) {
                int64_t val;
                int ret = rv.GetTimestamp(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kBigInt) {
                int64_t val;
                int ret = rv.GetInt64(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kBool) {
                bool val = 0;
                int ret = rv.GetBool(i, &val);
                if (ret == 0) {
                    if (val) col = "true";
                    else
                        col = "false";
                }
            } else if (type == rtidb::type::kFloat) {
                float val;
                int ret = rv.GetFloat(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kSmallInt) {
                int16_t val;
                int ret = rv.GetInt16(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kDouble) {
                double val;
                int ret = rv.GetDouble(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kVarchar ||
                       type == rtidb::type::kString) {
                char* ch = NULL;
                uint32_t len = 0;
                int ret = rv.GetString(i, &ch, &len);
                if (ret == 0) {
                    col.assign(ch, len);
                }
            } else if (type == ::rtidb::type::kDate) {
                uint32_t year = 0;
                uint32_t month = 0;
                uint32_t day = 0;
                rv.GetDate(i, &year, &month, &day);
                std::stringstream ss;
                ss << year << "-" << month << "-" << day;
                col = ss.str();
            }
            value_vec->emplace_back(std::move(col));
        }
        return true;
    }
};
__attribute__((unused)) static bool FillTableRows(
        const std::string& data,
        uint32_t count,
        const Schema& schema,
        std::vector<std::vector<std::string>>* row_vec) {
    rtidb::codec::RowView rv(schema);
    uint32_t offset = 0;
    for (uint32_t i = 0; i < count; i++) {
        std::vector<std::string> row;
        const char* ch = data.c_str();
        ch += offset;
        uint32_t value_size = 0;
        memcpy(static_cast<void*>(&value_size), ch, 4);
        ch += 4;
        bool ok = rv.Reset(reinterpret_cast<int8_t*>(const_cast<char*>(ch)),
                value_size);
        if (!ok) {
            return false;
        }
        offset += 4 + value_size;
        if (!rtidb::codec::RowCodec::DecodeRow(schema, rv, row)) {
            return false;
        }
        for (uint64_t i = 0; i < row.size(); i++) {
            if (row[i] == rtidb::codec::NONETOKEN) {
                row[i] = "null";
            }
        }
        row_vec->push_back(std::move(row));
    }
    return true;
}
__attribute__((unused)) static void FillTableRow(
    uint32_t full_schema_size,
    const std::vector<::rtidb::codec::ColumnDesc>& base_schema, const char* row,
    const uint32_t row_size, std::vector<std::string>& vrow) {  // NOLINT
    rtidb::codec::FlatArrayIterator fit(row, row_size, base_schema.size());
    while (full_schema_size > 0) {
        std::string col;
        if (!fit.Valid()) {
            full_schema_size--;
            vrow.emplace_back("");
            continue;
        } else if (fit.GetType() == ::rtidb::codec::ColType::kString) {
            fit.GetString(&col);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kUInt16) {
            uint16_t uint16_col = 0;
            fit.GetUInt16(&uint16_col);
            col = boost::lexical_cast<std::string>(uint16_col);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kInt16) {
            int16_t int16_col = 0;
            fit.GetInt16(&int16_col);
            col = boost::lexical_cast<std::string>(int16_col);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kInt32) {
            int32_t int32_col = 0;
            fit.GetInt32(&int32_col);
            col = boost::lexical_cast<std::string>(int32_col);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kInt64) {
            int64_t int64_col = 0;
            fit.GetInt64(&int64_col);
            col = boost::lexical_cast<std::string>(int64_col);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kUInt32) {
            uint32_t uint32_col = 0;
            fit.GetUInt32(&uint32_col);
            col = boost::lexical_cast<std::string>(uint32_col);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kUInt64) {
            uint64_t uint64_col = 0;
            fit.GetUInt64(&uint64_col);
            col = boost::lexical_cast<std::string>(uint64_col);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kDouble) {
            double double_col = 0.0;
            fit.GetDouble(&double_col);
            col = boost::lexical_cast<std::string>(double_col);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kFloat) {
            float float_col = 0.0f;
            fit.GetFloat(&float_col);
            col = boost::lexical_cast<std::string>(float_col);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kTimestamp) {
            uint64_t ts = 0;
            fit.GetTimestamp(&ts);
            col = boost::lexical_cast<std::string>(ts);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kDate) {
            uint64_t dt = 0;
            fit.GetDate(&dt);
            time_t rawtime = (time_t)dt / 1000;
            tm* timeinfo = localtime(&rawtime);  // NOLINT
            char buf[20];
            strftime(buf, 20, "%Y-%m-%d", timeinfo);
            col.assign(buf);
        } else if (fit.GetType() == ::rtidb::codec::ColType::kBool) {
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
        vrow.emplace_back(std::move(col));
    }
}

__attribute__((unused)) static void FillTableRow(
    const std::vector<::rtidb::codec::ColumnDesc>& schema, const char* row,
    const uint32_t row_size,
    std::vector<std::string>& vrow) {  // NOLINT
    return FillTableRow(schema.size(), schema, row, row_size, vrow);
}

}  // namespace codec
}  // namespace rtidb
