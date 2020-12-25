/*
 * table_reader.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_SDK_TABLE_READER_H_
#define SRC_SDK_TABLE_READER_H_

#include <string>
#include <vector>
#include "sdk/result_set.h"

namespace rtidb {
namespace sdk {

struct ScanOption {
    std::string ts_name;
    std::string idx_name;
    uint32_t limit = 0;
    uint32_t at_least = 0;
    std::vector<std::string> projection;
};

class TableReader {

 public:
    TableReader() {}

    virtual ~TableReader() {}

    virtual std::shared_ptr<fesql::sdk::ResultSet> Scan(const std::string& db,
            const std::string& table, const std::string& key,
            int64_t st,
            int64_t et, const ScanOption& so);

};

} // sdk
} // rtidb

#endif  // SRC_SDK_TABLE_READER_H_

