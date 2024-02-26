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
#include <vector>

#include "proto/common.pb.h"
#include "storage/disk_table.h"
#include "storage/snapshot.h"
#include "storage/table.h"

namespace openmldb {
namespace storage {

class DiskTableSnapshot : public Snapshot {
 public:
    DiskTableSnapshot(uint32_t tid, uint32_t pid, const std::string& db_root_path);
    virtual ~DiskTableSnapshot() = default;
    bool Init() override;
    int MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset, uint64_t end_offset,
                     uint64_t term = 0) override;
    bool Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) override;

    base::Status ExtractIndexData(const std::shared_ptr<Table>& table,
            const std::vector<::openmldb::common::ColumnKey>& add_indexs,
            const std::vector<std::shared_ptr<::openmldb::log::WriteHandle>>& whs,
            uint64_t offset, bool dump_data) override;

 private:
    std::string db_root_path_;
};

}  // namespace storage
}  // namespace openmldb
