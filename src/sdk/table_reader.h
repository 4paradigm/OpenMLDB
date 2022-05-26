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

#ifndef SRC_SDK_TABLE_READER_H_
#define SRC_SDK_TABLE_READER_H_

#include <memory>
#include <string>
#include <vector>

#include "sdk/result_set.h"

namespace openmldb {
namespace sdk {

struct ScanOption {
    std::string idx_name;
    uint32_t limit = 0;
    std::vector<std::string> projection;
};

class ScanFuture {
 public:
    ScanFuture() {}
    virtual ~ScanFuture() {}
    virtual std::shared_ptr<hybridse::sdk::ResultSet> GetResultSet(hybridse::sdk::Status* status) = 0;
    virtual bool IsDone() const = 0;
};

class TableReader {
 public:
    TableReader() {}

    virtual ~TableReader() {}

    virtual std::shared_ptr<hybridse::sdk::ResultSet> Scan(const std::string& db, const std::string& table,
                                                           const std::string& key, int64_t st, int64_t et,
                                                           const ScanOption& so, hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::ScanFuture> AsyncScan(const std::string& db, const std::string& table,
                                                                 const std::string& key, int64_t st, int64_t et,
                                                                 const ScanOption& so, int64_t timeout_ms,
                                                                 hybridse::sdk::Status* status) = 0;
};

}  // namespace sdk
}  // namespace openmldb

#endif  // SRC_SDK_TABLE_READER_H_
