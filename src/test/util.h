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

#ifndef SRC_TEST_UTIL_H_
#define SRC_TEST_UTIL_H_

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "brpc/server.h"
#include "gflags/gflags.h"
#include "nameserver/name_server_impl.h"
#include "codec/sdk_codec.h"
#include "tablet/tablet_impl.h"

DECLARE_string(endpoint);
DECLARE_string(tablet);
DECLARE_string(zk_cluster);

namespace openmldb {
namespace test {

using ::openmldb::codec::SchemaCodec;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

void AddDefaultSchema(uint64_t abs_ttl, uint64_t lat_ttl, ::openmldb::type::TTLType ttl_type,
                      ::openmldb::nameserver::TableInfo* table_meta) {
    auto column_desc = table_meta->add_column_desc();
    column_desc->set_name("idx0");
    column_desc->set_data_type(::openmldb::type::kString);
    auto column_desc1 = table_meta->add_column_desc();
    column_desc1->set_name("value");
    column_desc1->set_data_type(::openmldb::type::kString);
    auto column_key = table_meta->add_column_key();
    column_key->set_index_name("idx0");
    column_key->add_col_name("idx0");
    ::openmldb::common::TTLSt* ttl_st = column_key->mutable_ttl();
    ttl_st->set_abs_ttl(abs_ttl);
    ttl_st->set_lat_ttl(lat_ttl);
    ttl_st->set_ttl_type(ttl_type);
}

void SetDimension(uint32_t id, const std::string& key, openmldb::api::Dimension* dim) {
    dim->set_idx(id);
    dim->set_key(key);
}

void AddDimension(uint32_t id, const std::string& key, ::openmldb::api::LogEntry* entry) {
    SetDimension(id, key, entry->add_dimensions());
}

std::string EncodeKV(const std::string& key, const std::string& value) {
    ::openmldb::api::TableMeta meta;
    meta.set_format_version(1);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "key", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "value", ::openmldb::type::kString);
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    std::string result;
    std::vector<std::string> row = {key, value};
    sdk_codec.EncodeRow(row, &result);
    return result;
}

std::string DecodeV(const std::string& value) {
    ::openmldb::api::TableMeta meta;
    meta.set_format_version(1);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "key", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "value", ::openmldb::type::kString);
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    std::vector<std::string> row;
    sdk_codec.DecodeRow(value, &row);
    return row[1];
}

::openmldb::api::TableMeta GetTableMeta(const std::vector<std::string>& fields) {
    ::openmldb::api::TableMeta meta;
    meta.set_format_version(1);
    for (const auto& field : fields) {
        SchemaCodec::SetColumnDesc(meta.add_column_desc(), field, ::openmldb::type::kString);
    }
    return meta;
}

::openmldb::api::LogEntry PackKVEntry(uint64_t offset, const std::string& key,
        const std::string& value, uint64_t ts, uint64_t term) {
    auto meta = GetTableMeta({"key", "value"});
    SchemaCodec::SetIndex(meta.add_column_key(), "key1", "key", "", ::openmldb::type::kAbsoluteTime, 10, 0);
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    std::string result;
    std::vector<std::string> row = {key, value};
    sdk_codec.EncodeRow(row, &result);
    ::openmldb::api::LogEntry entry;
    entry.set_log_index(offset);
    entry.set_value(result);
    auto dimension = entry.add_dimensions();
    dimension->set_key(key);
    dimension->set_idx(0);
    entry.set_ts(ts);
    entry.set_term(term);
    return entry;
}

bool StartNS(const std::string& endpoint, brpc::Server* server) {
    FLAGS_endpoint = endpoint;
    auto nameserver = new ::openmldb::nameserver::NameServerImpl();
    if (!nameserver->Init("")) {
        return false;
    }
    if (server->AddService(nameserver, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    brpc::ServerOptions options;
    if (server->Start(endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    sleep(2);
    return true;
}

bool StartNS(const std::string& endpoint, const std::string& tb_endpoint, brpc::Server* server) {
    FLAGS_endpoint = endpoint;
    FLAGS_tablet = tb_endpoint;
    FLAGS_zk_cluster = "";
    auto nameserver = new ::openmldb::nameserver::NameServerImpl();
    if (!nameserver->Init("")) {
        return false;
    }
    if (server->AddService(nameserver, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    brpc::ServerOptions option;
    if (server->Start(endpoint.c_str(), &option) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    sleep(2);
    return true;
}

bool StartTablet(const std::string& endpoint, brpc::Server* server) {
    FLAGS_endpoint = endpoint;
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    if (!tablet->Init("")) {
        return false;
    }
    if (server->AddService(tablet, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    brpc::ServerOptions option;
    if (server->Start(endpoint.c_str(), &option) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    if (!tablet->RegisterZK()) {
        return false;
    }
    sleep(2);
    return true;
}

}  // namespace test
}  // namespace openmldb
#endif  // SRC_TEST_UTIL_H_
