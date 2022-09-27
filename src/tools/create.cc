//
// tools/parse_log.cc
// Copyright (C) 2020 4paradigm.com
// Author wangbao
// Date 2020-07-06
//

#include <gflags/gflags.h>
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "client/ns_client.h"
#include "codec/schema_codec.h"
#include "google/protobuf/text_format.h"
#include "proto/name_server.pb.h"
#include "proto/rtidb_name_server.pb.h"
#include "zk/zk_client.h"

DEFINE_string(table_name, "", "Set the command");
DEFINE_string(rtidb_zk_cluster, "", "rtidb zk cluster");
DEFINE_string(rtidb_zk_root_path, "", "rtidb zk root path");
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_uint32(partition_num);
DECLARE_uint32(replica_num);

namespace openmldb {
namespace tools {

std::string DEFAULT_DB = "default_db";

::openmldb::type::TTLType ConvertTTLType(::openmldb::rtidb_api::TTLType ttl_type) {
    switch (ttl_type) {
        case ::openmldb::rtidb_api::TTLType::kAbsoluteTime:
              return ::openmldb::type::TTLType::kAbsoluteTime;
        case ::openmldb::rtidb_api::TTLType::kLatestTime:
              return ::openmldb::type::TTLType::kLatestTime;
        case ::openmldb::rtidb_api::TTLType::kAbsAndLat:
              return ::openmldb::type::TTLType::kAbsAndLat;
        case ::openmldb::rtidb_api::TTLType::kAbsOrLat:
              return ::openmldb::type::TTLType::kAbsOrLat;
        case ::openmldb::rtidb_api::TTLType::kRelativeTime:
              return ::openmldb::type::TTLType::kAbsoluteTime;
    }
    return ::openmldb::type::TTLType::kAbsoluteTime;
}

bool GetTableInfo(const std::string& name, std::vector<std::shared_ptr<::openmldb::rtidb_nameserver::TableInfo>>* table_info_vec) {
    if (table_info_vec == nullptr) {
        return false;
    }
    std::string zk_table_data_path = FLAGS_rtidb_zk_root_path + "/table/table_data";
    std::shared_ptr<::openmldb::zk::ZkClient> zk_client;
    zk_client = std::make_shared<::openmldb::zk::ZkClient>(
            FLAGS_rtidb_zk_cluster, "", 1000, "", FLAGS_rtidb_zk_root_path);
    if (!zk_client->Init()) {
        PDLOG(WARNING, "zk client init failed");
        return false;
    }
    std::vector<std::string> table_vec;
    std::vector<std::string> db_table_vec;
    if (!zk_client->GetChildren(zk_table_data_path, table_vec)) {
        if (zk_client->IsExistNode(zk_table_data_path) > 0) {
            PDLOG(WARNING, "table data node is not exist");
        } else {
            PDLOG(WARNING, "get table name failed!");
        }
        return false;
    }
    for (const auto& table_name : table_vec) {
        std::string table_name_node = zk_table_data_path + "/" + table_name;
        std::string value;
        if (!zk_client->GetNodeValue(table_name_node, value)) {
            PDLOG(WARNING, "get table info failed! name[%s] table node[%s]", table_name.c_str(),
                  table_name_node.c_str());
            continue;
        }
        std::shared_ptr<::openmldb::rtidb_nameserver::TableInfo> table_info =
            std::make_shared<::openmldb::rtidb_nameserver::TableInfo>();
        if (!table_info->ParseFromString(value)) {
            PDLOG(WARNING, "parse table info failed! name[%s] value[%s] value size[%d]", table_name.c_str(),
                  value.c_str(), value.length());
            continue;
        }
        if (!name.empty()) {
            if (table_info->name() == name) {
                table_info_vec->emplace_back(table_info);
                break;
            }
        } else {
            table_info_vec->emplace_back(table_info);
        }
    }
    return true;
}

bool SetDataType(const std::string& type, ::openmldb::common::ColumnDesc* column_desc) {
    auto iter = openmldb::codec::DATA_TYPE_MAP.find(type);
    if (iter == openmldb::codec::DATA_TYPE_MAP.end()) {
        PDLOG(WARNING, "invalid type %s", type.c_str());
        return false;
    }
    column_desc->set_data_type(iter->second);
    return true;
}

bool ConvertTableInfo(const ::openmldb::rtidb_nameserver::TableInfo& table_info,
        std::shared_ptr<::openmldb::nameserver::TableInfo> openmldb_table_info) {
    openmldb_table_info->set_db(DEFAULT_DB);
    openmldb_table_info->set_name(table_info.name());
    openmldb_table_info->set_partition_num(FLAGS_partition_num);
    openmldb_table_info->set_replica_num(FLAGS_replica_num);
    if (table_info.has_compress_type() &&
            table_info.compress_type() == ::openmldb::rtidb_nameserver::CompressType::kSnappy) {
        openmldb_table_info->set_compress_type(::openmldb::type::CompressType::kSnappy);
    }
    ::openmldb::common::TTLSt table_ttl;
    std::map<std::string, ::openmldb::common::TTLSt> ttl_map;
    if (table_info.has_ttl_desc()) {
        table_ttl.set_abs_ttl(table_info.ttl_desc().abs_ttl());
        table_ttl.set_lat_ttl(table_info.ttl_desc().lat_ttl());
        table_ttl.set_ttl_type(ConvertTTLType(table_info.ttl_desc().ttl_type()));
    } else {
        ::openmldb::type::TTLType ttl_type;
        if (!::openmldb::codec::SchemaCodec::TTLTypeParse(table_info.ttl_type(), &ttl_type)) {
            PDLOG(WARNING, "parse ttl_type %s failed. table is %s", table_info.ttl_type().c_str(), table_info.name().c_str());
            return false;
        }
        table_ttl.set_ttl_type(ttl_type);
        if (ttl_type == openmldb::type::kAbsoluteTime) {
            table_ttl.set_abs_ttl(table_info.ttl());
        } else {
            table_ttl.set_lat_ttl(table_info.ttl());
        }
    }
    std::vector<std::string> index_cols;
    if (table_info.column_desc_v1_size() > 0) {
        for (int idx = 0; idx < table_info.column_desc_v1_size(); idx++) {
            auto column_desc = openmldb_table_info->add_column_desc();
            column_desc->set_name(table_info.column_desc_v1(idx).name());
            SetDataType(table_info.column_desc_v1(idx).type(), column_desc);
            ::openmldb::common::TTLSt table_ttl;
            if (table_info.column_desc_v1(idx).has_abs_ttl()) {
                table_ttl.set_abs_ttl(table_info.column_desc_v1(idx).abs_ttl());
            }
            if (table_info.column_desc_v1(idx).has_lat_ttl()) {
                table_ttl.set_lat_ttl(table_info.column_desc_v1(idx).lat_ttl());
            }
            if (table_ttl.has_abs_ttl() || table_ttl.has_lat_ttl()) {
                ttl_map.emplace(column_desc->name(), table_ttl);
            }
            if (table_info.column_desc_v1(idx).add_ts_idx()) {
                index_cols.emplace_back(column_desc->name());
            }
        }
    } else {
        for (int idx = 0; idx < table_info.column_desc_size(); idx++) {
            auto column_desc = openmldb_table_info->add_column_desc();
            column_desc->set_name(table_info.column_desc(idx).name());
            SetDataType(table_info.column_desc(idx).type(), column_desc);
            if (table_info.column_desc(idx).add_ts_idx()) {
                index_cols.emplace_back(column_desc->name());
            }
        }
    }
    for (int idx = 0; idx < table_info.added_column_desc_size(); idx++) {
        auto column_desc = openmldb_table_info->add_column_desc();
        column_desc->set_name(table_info.added_column_desc(idx).name());
        SetDataType(table_info.added_column_desc(idx).type(), column_desc);
    }
    if (table_info.column_key_size() > 0) {
        for (int idx = 0; idx < table_info.column_key_size(); idx++) {
            auto column_key = openmldb_table_info->add_column_key();
            column_key->set_index_name(table_info.column_key(idx).index_name());
            if (table_info.column_key(idx).col_name_size() == 0) {
                column_key->add_col_name(table_info.column_key(idx).index_name());
            } else {
                for (const auto& col_name : table_info.column_key(idx).col_name()) {
                    column_key->add_col_name(col_name);
                }
            }
            auto ttl = column_key->mutable_ttl();
            ttl->CopyFrom(table_ttl);
            if (table_info.column_key(idx).ts_name_size() > 0) {
                if (table_info.column_key(idx).ts_name_size() > 1) {
                    PDLOG(WARNING, "has multi ts in one index");
                    return false;
                }
                std::string ts_name = table_info.column_key(idx).ts_name(0);
                column_key->set_ts_name(ts_name);
                auto ttl_iter = ttl_map.find(ts_name);
                if (ttl_iter != ttl_map.end()) {
                    ttl->set_abs_ttl(ttl_iter->second.abs_ttl());
                    ttl->set_lat_ttl(ttl_iter->second.lat_ttl());
                }
            }
        }
    } else {
        for (const auto& col_name : index_cols) {
            auto column_key = openmldb_table_info->add_column_key();
            column_key->set_index_name(col_name);
            column_key->add_col_name(col_name);
            auto ttl = column_key->mutable_ttl();
            ttl->CopyFrom(table_ttl);
        }
    }
    return true;
}

std::shared_ptr<::openmldb::client::NsClient> GetNsClient() {
    std::string endpoint;
    std::string real_endpoint;
    std::shared_ptr<::openmldb::zk::ZkClient> zk_client;
    zk_client = std::make_shared<::openmldb::zk::ZkClient>(FLAGS_zk_cluster, "", 1000, "", FLAGS_zk_root_path);
    if (!zk_client->Init()) {
        PDLOG(WARNING, "zk client init failed");
        return {};
    }
    std::string node_path = FLAGS_zk_root_path + "/leader";
    std::vector<std::string> children;
    if (!zk_client->GetChildren(node_path, children) || children.empty()) {
        PDLOG(WARNING, "get children failed");
        return {};
    }
    std::string leader_path = node_path + "/" + children[0];
    if (!zk_client->GetNodeValue(leader_path, endpoint)) {
        PDLOG(WARNING, "get leader failed");
        return {};
    }
    auto client = std::make_shared<::openmldb::client::NsClient>(endpoint, endpoint);
    if (client->Init() < 0) {
        PDLOG(WARNING, "client init failed");
        return {};
    }
    return client;
}

void CreateTable() {
    auto client = GetNsClient();
    if (!client) {
        return;
    }
    std::string msg;
    if (!client->CreateDatabase(DEFAULT_DB, msg, true)) {
        PDLOG(WARNING, "create database failed. msg is %s", msg.c_str());
        return;
    }
    std::vector<std::shared_ptr<::openmldb::rtidb_nameserver::TableInfo>> tables;
    GetTableInfo(FLAGS_table_name, &tables);
    std::vector<std::shared_ptr<::openmldb::nameserver::TableInfo>> openmldb_tables;
    for (const auto& table : tables) {
        auto openmldb_table_info = std::make_shared<::openmldb::nameserver::TableInfo>();
        if (!ConvertTableInfo(*table, openmldb_table_info)) {
            PDLOG(WARNING, "convert table %s failed.", table->name().c_str());
            continue;
        }
        // std::string table_meta_info;
        // google::protobuf::TextFormat::PrintToString(*table, &table_meta_info);
        if (!client->CreateTable(*openmldb_table_info, true, msg)) {
            PDLOG(WARNING, "create %s failed. msg is %s", openmldb_table_info->name().c_str(), msg.c_str());
        } else {
            PDLOG(INFO, "create %s success", openmldb_table_info->name().c_str());
        }
    }
}

}  // namespace tools
}  // namespace openmldb

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::tools::CreateTable();
    return 0;
}
