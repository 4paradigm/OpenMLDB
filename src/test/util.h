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

#include <string>
#include <vector>

#include "tablet/tablet_impl.h"

namespace openmldb {
namespace test {

std::string GenRand();

class TempPath {
 public:
    TempPath();
    ~TempPath();
    std::string GetTempPath();
    std::string GetTempPath(const std::string& prefix);
    std::string CreateTempPath(const std::string& prefix);

 private:
    std::string base_path_;
};

void AddDefaultSchema(uint64_t abs_ttl, uint64_t lat_ttl, ::openmldb::type::TTLType ttl_type,
                      ::openmldb::nameserver::TableInfo* table_meta);

void SetDimension(uint32_t id, const std::string& key, openmldb::api::Dimension* dim);

void AddDimension(uint32_t id, const std::string& key, ::openmldb::api::LogEntry* entry);

std::string EncodeKV(const std::string& key, const std::string& value);

std::string DecodeV(const std::string& value);

::openmldb::api::TableMeta GetTableMeta(const std::vector<std::string>& fields);

::openmldb::api::LogEntry PackKVEntry(uint64_t offset, const std::string& key, const std::string& value, uint64_t ts,
                                      uint64_t term);

bool StartNS(const std::string& endpoint, brpc::Server* server);

bool StartNS(const std::string& endpoint, const std::string& tb_endpoint, brpc::Server* server);

bool StartTablet(const std::string& endpoint, brpc::Server* server);

void ProcessSQLs(sdk::SQLRouter* sr, std::initializer_list<absl::string_view> sqls);

// expect object for ResultSet cell
struct CellExpectInfo {
    CellExpectInfo() {}

    CellExpectInfo(const char* buf) : expect_(buf) {}  // NOLINT

    CellExpectInfo(const std::string& expect) : expect_(expect) {}  // NOLINT

    CellExpectInfo(const std::optional<std::string>& expect) : expect_(expect) {}  // NOLINT

    CellExpectInfo(const std::optional<std::string>& expect, const std::optional<std::string>& expect_not)
        : expect_(expect), expect_not_(expect_not) {}

    CellExpectInfo& operator=(const CellExpectInfo& other) {
        expect_ = other.expect_;
        expect_not_ = other.expect_not_;
        return *this;
    }

    // result string for target cell to match exactly
    // if empty, do not check
    std::optional<std::string> expect_;

    // what string target cell string should not match
    // if empty, do not check
    std::optional<std::string> expect_not_;
};

// expect the output of a ResultSet, first row is schema, all compared in string
// if expect[i][j].(expect_|expect_not_) is not set, assert will skip
void ExpectResultSetStrEq(const std::vector<std::vector<CellExpectInfo>>& expect, hybridse::sdk::ResultSet* rs,
                          bool ordered = true);

std::string GetExeDir();

std::string GetParentDir(const std::string& path);

api::TableMeta CreateTableMeta(const std::string& name, uint32_t tid, uint32_t pid,
            uint64_t abs_ttl, uint64_t lat_ttl,
            bool leader, const std::vector<std::string>& endpoints,
            const ::openmldb::type::TTLType& type, uint32_t seg_cnt, uint64_t term,
            ::openmldb::type::CompressType compress_type,
            common::StorageMode storage_mode = ::openmldb::common::kMemory);


}  // namespace test
}  // namespace openmldb
#endif  // SRC_TEST_UTIL_H_
