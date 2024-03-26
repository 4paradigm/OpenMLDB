/**
 * Copyright (c) 2023 OpenMLDB authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "test/util.h"

#include <algorithm>
#include <filesystem>
#include <map>
#include <memory>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "brpc/server.h"
#include "codec/sdk_codec.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"

DECLARE_string(endpoint);
DECLARE_string(tablet);
DECLARE_string(zk_cluster);
DECLARE_string(db_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(recycle_bin_ssd_root_path);
DECLARE_string(recycle_bin_root_path);
DECLARE_string(recycle_bin_hdd_root_path);


namespace openmldb {
namespace test {

using ::openmldb::codec::SchemaCodec;

struct DiskFlagsRAII {
    // no copy
    DiskFlagsRAII(const DiskFlagsRAII&) = delete;
    DiskFlagsRAII(DiskFlagsRAII&&) = default;
    DiskFlagsRAII& operator=(const DiskFlagsRAII&) = delete;
    DiskFlagsRAII& operator=(DiskFlagsRAII&&) = default;

    explicit DiskFlagsRAII(const std::filesystem::path& p) : prefix(p) {
        LOG(INFO) << "setting temp path for test in " << prefix;
        FLAGS_db_root_path = prefix / "db_root";
        FLAGS_ssd_root_path = prefix / "ssd_root";
        FLAGS_hdd_root_path = prefix / "hdd_root";
        FLAGS_recycle_bin_root_path = prefix / "recycle_root";
        FLAGS_recycle_bin_hdd_root_path = prefix / "recycle_hdd_root";
        FLAGS_recycle_bin_ssd_root_path = prefix / "recycle_ssd_root";
    }

    ~DiskFlagsRAII() {
        LOG(INFO) << "removing temp path: " << prefix;
        std::filesystem::remove_all(prefix);
    }

    std::filesystem::path prefix;
};
static const DiskFlagsRAII* flags_raii_ptr = nullptr;

std::string GenRand() {
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
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "key", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(meta.add_column_desc(), "value", ::openmldb::type::kString);
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    std::vector<std::string> row;
    sdk_codec.DecodeRow(value, &row);
    return row[1];
}

::openmldb::api::TableMeta GetTableMeta(const std::vector<std::string>& fields) {
    ::openmldb::api::TableMeta meta;
    for (const auto& field : fields) {
        SchemaCodec::SetColumnDesc(meta.add_column_desc(), field, ::openmldb::type::kString);
    }
    return meta;
}

::openmldb::api::LogEntry PackKVEntry(uint64_t offset, const std::string& key, const std::string& value, uint64_t ts,
                                      uint64_t term) {
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

void ProcessSQLs(sdk::SQLRouter* sr, std::initializer_list<absl::string_view> sqls) {
    hybridse::sdk::Status status;
    for (auto sql : sqls) {
        sr->ExecuteSQL(std::string(sql), &status);
        EXPECT_TRUE(status.IsOK()) << "running sql=" << sql << " failed: code=" << status.code << ", msg=" << status.msg
                                   << "\n"
                                   << status.trace;
    }
}

void ExpectResultSetStrEq(const std::vector<std::vector<CellExpectInfo>>& expect, hybridse::sdk::ResultSet* rs,
                          bool ordered) {
    ASSERT_EQ(expect.size(), static_cast<uint64_t>(rs->Size()) + 1);
    size_t idx = 0;
    // schema check
    ASSERT_EQ(expect.front().size(), rs->GetSchema()->GetColumnCnt());
    for (size_t i = 0; i < expect[idx].size(); ++i) {
        ASSERT_TRUE(expect[idx][i].expect_.has_value());
        EXPECT_EQ(expect[idx][i].expect_.value(), rs->GetSchema()->GetColumnName(i));
    }

    while (++idx < expect.size() && rs->Next()) {
        for (size_t i = 0; i < expect[idx].size(); ++i) {
            std::string val;
            EXPECT_TRUE(rs->GetAsString(i, val));
            if (ordered) {
                if (expect[idx][i].expect_.has_value()) {
                    EXPECT_EQ(expect[idx][i].expect_.value(), val) << "expect[" << idx << "][" << i << "]";
                }
                if (expect[idx][i].expect_not_.has_value()) {
                    EXPECT_NE(expect[idx][i].expect_not_.value(), val) << "expect_not[" << idx << "][" << i << "]";
                }
            } else {
                bool matched = false;
                size_t null_counter = 0;
                EXPECT_FALSE(expect[idx][i].expect_not_.has_value());
                for (size_t j = 1; j < expect.size(); j++) {
                    if (expect[j][i].expect_.has_value()) {
                        if (expect[j][i].expect_.value() == val) {
                            matched = true;
                            break;
                        }
                    } else {
                        null_counter++;
                    }
                }
                EXPECT_TRUE(matched || null_counter == expect.size() - 1)
                    << "rs[" << idx << "][" << i << "]: " << val << " not matched";
            }
        }
    }
}

std::string GetExeDir() {
    char path[1024];
    int cnt = readlink("/proc/self/exe", path, 1024);
    if (cnt < 0 || cnt >= 1024) {
        return "";
    }
    for (int i = cnt; i >= 0; i--) {
        if (path[i] == '/') {
            path[i + 1] = '\0';
            break;
        }
    }
    return std::string(path);
}

std::string GetParentDir(const std::string& path) {
    if (path.empty()) {
        return "";
    }
    std::string dir = path;
    if (dir.back() == '/') {
        dir.pop_back();
    }
    auto pos = dir.find_last_of('/');
    if (pos != std::string::npos && pos != 0) {
        return dir.substr(0, pos);
    }
    return dir;
}

// TempPath requires initialize random DiskFlagsRAII explicitly,
// or abort with error message
TempPath::TempPath() {
    assert(flags_raii_ptr != nullptr && "use of TempPath must explicitly initialize with InitRandomDiskFlags()");
    auto& global_tmp_prefix = flags_raii_ptr->prefix;
    std::filesystem::path tmp_path = global_tmp_prefix / absl::StrCat("tmppath", ::openmldb::test::GenRand());
    base_path_ = tmp_path.string();
}
TempPath::~TempPath() {}

std::string TempPath::GetTempPath() { return absl::StrCat(base_path_, "/", ::openmldb::test::GenRand()); }

std::string TempPath::GetTempPath(const std::string& prefix) {
    return absl::StrCat(base_path_, "/", prefix, ::openmldb::test::GenRand());
}

std::string TempPath::CreateTempPath(const std::string& prefix) {
    std::string path = GetTempPath(prefix);
    std::filesystem::create_directories(path);
    return path;
}

api::TableMeta CreateTableMeta(const std::string& name, uint32_t tid, uint32_t pid,
        uint64_t abs_ttl, uint64_t lat_ttl,
        bool leader, const std::vector<std::string>& endpoints,
        const ::openmldb::type::TTLType& type, uint32_t seg_cnt, uint64_t term,
        ::openmldb::type::CompressType compress_type,
        ::openmldb::common::StorageMode storage_mode) {
    api::TableMeta table_meta;
    table_meta.set_name(name);
    table_meta.set_tid(tid);
    table_meta.set_pid(pid);
    table_meta.set_compress_type(compress_type);
    table_meta.set_seg_cnt(seg_cnt);
    table_meta.set_storage_mode(storage_mode);
    if (leader) {
        table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
        table_meta.set_term(term);
    } else {
        table_meta.set_mode(::openmldb::api::TableMode::kTableFollower);
    }
    for (size_t i = 0; i < endpoints.size(); i++) {
        table_meta.add_replicas(endpoints[i]);
    }
    ::openmldb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("idx0");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnKey* index = table_meta.add_column_key();
    index->set_index_name("idx0");
    index->add_col_name("idx0");
    ::openmldb::common::TTLSt* ttl = index->mutable_ttl();
    ttl->set_abs_ttl(abs_ttl);
    ttl->set_lat_ttl(lat_ttl);
    ttl->set_ttl_type(type);
    return table_meta;
}


static std::filesystem::path GenerateTmpDirPrefix(absl::string_view tag) {
    std::filesystem::path tmp_path = std::filesystem::temp_directory_path() / "openmldb";
    const auto& rand = ::openmldb::test::GenRand();
    return tmp_path / absl::StrCat(tag, rand);
}

static DiskFlagsRAII GenerateRandomDiskFlags(absl::string_view tag) {
    return DiskFlagsRAII(GenerateTmpDirPrefix(tag));
}

static bool InitRandomDiskFlagsImpl(absl::string_view tag) {
    static DiskFlagsRAII flags_raii = GenerateRandomDiskFlags(tag);
    auto sig_handler = [](int sig) {
        flags_raii.~DiskFlagsRAII();
        _exit(1);
    };
    signal(SIGINT, sig_handler);
    signal(SIGKILL, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGABRT, sig_handler);

    flags_raii_ptr = &flags_raii;
    return true;
}

// DiskFlagsRAII instance that initialize exact once:
// - flags_raii ensure global gflags define and set once
// - flags_raii_flag also ensure signal handler set once
bool InitRandomDiskFlags(std::string_view tag) {
    static bool flags_raii_flag = InitRandomDiskFlagsImpl(tag);
    return flags_raii_flag;
}
}  // namespace test
}  // namespace openmldb
