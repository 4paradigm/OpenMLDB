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

#include <memory>
#include <string>

#include "log/log_reader.h"
#include "log/log_writer.h"
#include "storage/table.h"

typedef ::openmldb::base::Skiplist<uint32_t, uint64_t, ::openmldb::base::DefaultComparator> LogParts;

namespace openmldb {
namespace storage {

class Binlog {
 public:
    Binlog(LogParts* log_part, const std::string& binlog_path);
    ~Binlog() = default;
    bool RecoverFromBinlog(std::shared_ptr<Table> table, uint64_t offset,
                           uint64_t& latest_offset);  // NOLINT

 private:
    LogParts* log_part_;
    std::string log_path_;
};

}  // namespace storage
}  // namespace openmldb
