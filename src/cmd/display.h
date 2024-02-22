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

#include "base/texttable.h"
#include "boost/regex.hpp"
#include "cmd/sdk_iterator.h"
#include "codec/row_codec.h"
#include "codec/schema_codec.h"
#include "codec/sdk_codec.h"
#include "common/tprinter.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "sdk/base_impl.h"
#include "storage/segment.h"
#include "vm/catalog.h"

DECLARE_uint32(max_col_display_length);

namespace openmldb {
namespace cmd {

static void TransferString(std::vector<std::string>* vec) {
    std::for_each(vec->begin(), vec->end(), [](std::string& str) {
        if (str == ::openmldb::codec::NONETOKEN) {
            str = "-";
        } else if (str == ::openmldb::codec::EMPTY_STRING) {
            str = "";
        }
    });
}

__attribute__((unused)) static void PrintSchema(
    const google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>& column_desc,
    const google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>& added_column_desc,
    std::ostream& stream) {
    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add("#");
    t.add("Field");
    t.add("Type");
    t.add("Null");
    t.add("Default");
    t.end_of_row();

    for (int i = 0; i < column_desc.size(); i++) {
        const auto& column = column_desc.Get(i);
        t.add(std::to_string(i + 1));
        t.add(column.name());
        // kXXX discard k
        t.add(DataType_Name(column.data_type()).substr(1));
        t.add(column.not_null() ? "NO" : "YES");
        t.add(column.default_value());
        t.end_of_row();
    }

    for (int i = 0; i < added_column_desc.size(); i++) {
        const auto& column = added_column_desc.Get(i);
        t.add(std::to_string(column_desc.size() + i + 1));
        t.add(column.name());
        // kXXX discard k
        t.add(DataType_Name(column.data_type()).substr(1));
        t.add(column.not_null() ? "NO" : "YES");
        t.end_of_row();
    }
    stream << t;
}

__attribute__((unused)) static void PrintSchema(
    const google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>& column_desc_field,
    std::ostream& stream) {
    PrintSchema(column_desc_field, {}, stream);
}

__attribute__((unused)) static void PrintColumnKey(
    const google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>& column_key_field,
    std::ostream& stream) {
    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add("#");
    t.add("name");
    t.add("keys");
    t.add("ts");
    t.add("ttl");
    t.add("ttl_type");
    t.end_of_row();
    int index_pos = 1;
    for (int i = 0; i < column_key_field.size(); i++) {
        const auto& column_key = column_key_field.Get(i);
        if (column_key.flag() == 1) {
            continue;
        }
        t.add(std::to_string(index_pos));
        index_pos++;
        t.add(column_key.index_name());
        std::string key;
        for (const auto& name : column_key.col_name()) {
            if (key.empty()) {
                key = name;
            } else {
                key += "|" + name;
            }
        }
        t.add((key.empty() ? column_key.index_name() : key));

        if (!column_key.ts_name().empty()) {
            t.add(column_key.ts_name());
        } else {
            t.add("-");
        }

        if (column_key.has_ttl()) {
            std::ostringstream oss;
            auto& ttl = column_key.ttl();
            storage::TTLSt ttl_st(ttl);
            t.add(ttl_st.ToString());
            t.add(TTLType_Name(ttl_st.GetProtoTTLType()));
        } else {
            t.add("-");  // ttl
            t.add("-");  // ttl_type
        }

        t.end_of_row();
    }
    stream << t;
}

__attribute__((unused)) static void ShowTableRows(::openmldb::codec::SDKCodec* codec,
                                                  ::openmldb::cmd::SDKIterator* it) {
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
        openmldb::base::Slice data = it->GetValue();
        std::string value(data.data(), data.size());
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

__attribute__((unused)) static void ShowTableRows(const ::openmldb::api::TableMeta& table_info,
                                                  ::openmldb::cmd::SDKIterator* it) {
    ::openmldb::codec::SDKCodec codec(table_info);
    ShowTableRows(&codec, it);
}

__attribute__((unused)) static void ShowTableRows(const ::openmldb::nameserver::TableInfo& table_info,
                                                  ::openmldb::cmd::SDKIterator* it) {
    ::openmldb::codec::SDKCodec codec(table_info);
    ShowTableRows(&codec, it);
}

__attribute__((unused)) static void ShowTableRows(const std::string& key, ::openmldb::cmd::SDKIterator* it) {
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

__attribute__((unused)) static void PrintTableInfo(const std::vector<::openmldb::nameserver::TableInfo>& tables) {
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
                if (value.table_partition(idx).partition_meta(meta_idx).is_alive()) {
                    row.push_back("yes");
                } else {
                    row.push_back("no");
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
                    row.push_back(::openmldb::base::HumanReadableString(
                        value.table_partition(idx).partition_meta(meta_idx).record_byte_size()));
                } else {
                    row.push_back("-");
                }
                row.push_back(::openmldb::base::HumanReadableString(
                    value.table_partition(idx).partition_meta(meta_idx).diskused()));
                tp.AddRow(row);
            }
        }
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintTableStatus(const std::vector<::openmldb::api::TableStatus>& status_vec) {
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
        row.push_back(::openmldb::api::TableMode_Name(table_status.mode()));
        row.push_back(::openmldb::api::TableState_Name(table_status.state()));
        if (table_status.is_expire()) {
            row.push_back("true");
        } else {
            row.push_back("false");
        }
        row.push_back("0min");
        row.push_back(std::to_string(table_status.time_offset()) + "s");
        row.push_back(::openmldb::base::HumanReadableString(table_status.record_byte_size() +
                                                            table_status.record_idx_byte_size()));
        row.push_back(::openmldb::type::CompressType_Name(table_status.compress_type()));
        row.push_back(std::to_string(table_status.skiplist_height()));
        tp.AddRow(row);
    }
    tp.Print(true);
}

__attribute__((unused)) static void PrintTableInformation(
    std::vector<::openmldb::nameserver::TableInfo>& tables) {  // NOLINT
    if (tables.empty()) {
        return;
    }
    ::openmldb::nameserver::TableInfo table = tables[0];
    std::vector<std::string> row;
    row.push_back("attribute");
    row.push_back("value");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    ::openmldb::common::TTLSt ttl_st;
    std::string name = table.name();
    std::string replica_num = std::to_string(table.replica_num());
    std::string partition_num = std::to_string(table.partition_num());
    std::string compress_type = ::openmldb::type::CompressType_Name(table.compress_type());
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
    row.push_back(::openmldb::base::HumanReadableString(memused));
    tp.AddRow(row);
    row.clear();
    row.push_back("diskused");
    row.push_back(::openmldb::base::HumanReadableString(diskused));
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

__attribute__((unused)) static void PrintDatabase(const std::vector<std::string>& dbs) {
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

__attribute__((unused)) static void PrintOfflineTableInfo(
        const ::openmldb::nameserver::OfflineTableInfo& offline_table_info,
        std::ostream& stream) {
    ::hybridse::base::TextTable t('-', ' ', ' ');

    t.add("Data path");
    t.add("Symbolic paths");
    t.add("Format");
    t.add("Options");
    t.end_of_row();

    auto& options = offline_table_info.options();
    std::string optionStr;
    bool first = true;
    for (auto& pair : options) {
        if (first) {
            optionStr += pair.first + ":" + pair.second;
            first = false;
        } else {
            optionStr += ", " + pair.first + ":" + pair.second;
        }
    }

    std::string offline_symbolic_paths = "";
    auto symbolic_paths = offline_table_info.symbolic_paths();
    int symbolic_paths_size = offline_table_info.symbolic_paths_size();
    for (int i = 0; i < symbolic_paths_size; i++) {
        offline_symbolic_paths += symbolic_paths.Get(i);
        if (i != symbolic_paths_size - 1) {
            offline_symbolic_paths += ", ";
        }
    }

    t.add(offline_table_info.path());
    t.add(offline_symbolic_paths);
    t.add(offline_table_info.format());
    t.add(optionStr);
    t.end_of_row();

    stream << t << std::endl;
}

__attribute__((unused)) static void PrintTableOptions(
    const std::unordered_map<std::string, std::string>& options,
    std::ostream& stream) {
    ::hybridse::base::TextTable t('-', ' ', ' ');

    for (const auto& kv : options) {
        t.add(kv.first);
    }
    t.end_of_row();

    for (auto& kv : options) {
        t.add(kv.second);
    }
    t.end_of_row();

    stream << t << std::endl;
}

__attribute__((unused)) static void PrintTableSchema(const ::hybridse::vm::Schema& schema,
        std::ostream& stream) {
    if (schema.empty()) {
        stream << "Empty set" << std::endl;
        return;
    }

    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add("#");
    t.add("Field");
    t.add("Type");
    t.add("Null");
    t.end_of_row();

    for (auto i = 0; i < schema.size(); i++) {
        const auto& column = schema.Get(i);
        t.add(std::to_string(i + 1));
        t.add(column.name());
        t.add(::hybridse::type::Type_Name(column.type()));
        t.add(column.is_not_null() ? "NO" : "YES");
        t.end_of_row();
    }
    stream << t;
}

__attribute__((unused)) static void PrintItemTable(const std::vector<std::string>& head,
                    const std::vector<std::vector<std::string>>& items, bool transpose,
                    std::ostream& stream) {
    if (items.empty()) {
        stream << "Empty set" << std::endl;
        return;
    }
    DLOG(INFO) << "table size " << items.size() << "-" << items[0].size();
    DCHECK(transpose ? (head.size() == items.size()) : (head.size() == items[0].size()));
    ::hybridse::base::TextTable t('-', ' ', ' ');
    std::for_each(head.begin(), head.end(), [&t](auto& item) { t.add(item); });
    t.end_of_row();
    if (transpose) {
        // flip along the major diagonal (top left to bottom right)
        for (size_t i = 0; i < items[0].size(); ++i) {
            // print the i column
            std::for_each(items.begin(), items.end(), [&t, &i](auto& row) { t.add(row[i]); });
            t.end_of_row();
        }
    } else {
        for (const auto& line : items) {
            std::for_each(line.begin(), line.end(), [&t](auto& item) { t.add(item); });
            t.end_of_row();
        }
    }

    stream << t;
    auto items_size = transpose ? items[0].size() : items.size();
    if (items_size > 1) {
        stream << items_size << " rows in set" << std::endl;
    } else {
        stream << items_size << " row in set" << std::endl;
    }
}

__attribute__((unused)) static void PrintItemTable(const std::vector<std::string>& head,
                    const std::vector<std::vector<std::string>>& items,
                    std::ostream& stream) {
    PrintItemTable(head, items, false, stream);
}

__attribute__((unused)) static void PrintProcedureSchema(const std::string& head,
        const ::hybridse::sdk::Schema& sdk_schema,
        std::ostream& stream) {
    try {
        const auto& schema_impl = dynamic_cast<const ::hybridse::sdk::SchemaImpl&>(sdk_schema);
        auto& schema = schema_impl.GetSchema();
        if (schema.empty()) {
            stream << "Empty set" << std::endl;
            return;
        }
        stream << "# " << head << std::endl;

        ::hybridse::base::TextTable t('-', ' ', ' ');
        t.add("#");
        t.add("Field");
        t.add("Type");
        t.add("IsConstant");
        t.end_of_row();

        for (auto i = 0; i < schema.size(); i++) {
            const auto& column = schema.Get(i);
            t.add(std::to_string(i + 1));
            t.add(column.name());
            t.add(::hybridse::type::Type_Name(column.type()).substr(1));
            t.add(column.is_constant() ? "YES" : "NO");
            t.end_of_row();
        }
        stream << t << std::endl;
    } catch (std::bad_cast&) {
        return;
    }
}

__attribute__((unused)) static void PrintProcedureInfo(
        const hybridse::sdk::ProcedureInfo& sp_info,
        std::ostream& stream) {
    std::vector<std::string> vec{sp_info.GetDbName(), sp_info.GetSpName()};

    std::string type_name = "SP";
    std::string sql = sp_info.GetSql();

    if (sp_info.GetType() == hybridse::sdk::kReqDeployment) {
        type_name = "Deployment";
        std::string pattern_sp = "CREATE PROCEDURE";
        sql = boost::regex_replace(sql, boost::regex(pattern_sp), "DEPLOY");
        std::string pattern_blank = "(.*)(\\(.*\\) )(BEGIN )(.*)( END;)";
        sql = boost::regex_replace(sql, boost::regex(pattern_blank), "$1$4");
        if (!sp_info.GetOption()->empty()) {
            std::stringstream ss;
            ss << " OPTIONS(";
            for (auto iter = sp_info.GetOption()->begin(); iter != sp_info.GetOption()->end(); iter++) {
                if (iter != sp_info.GetOption()->begin()) {
                    ss << ", ";
                }
                ss << absl::AsciiStrToUpper(iter->first) << "=\"" << iter->second << "\"";
            }
            ss << ")";
            std::string prefix = absl::StrCat("DEPLOY ", sp_info.GetSpName());
            absl::string_view old_sql = sql;
            old_sql.remove_prefix(prefix.size());
            sql = absl::StrCat(prefix, ss.str(), old_sql);
        }
    }

    PrintItemTable({"DB", type_name}, {vec}, stream);
    std::vector<std::string> items{sql};
    PrintItemTable({"SQL"}, {items}, true, stream);
    PrintProcedureSchema("Input Schema", sp_info.GetInputSchema(), stream);
    PrintProcedureSchema("Output Schema", sp_info.GetOutputSchema(), stream);
}

}  // namespace cmd
}  // namespace openmldb
