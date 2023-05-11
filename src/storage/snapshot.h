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

#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include "storage/table.h"

namespace openmldb {
namespace storage {

class Snapshot {
 public:
    Snapshot(uint32_t tid, uint32_t pid) : tid_(tid), pid_(pid), offset_(0), making_snapshot_(false) {}
    virtual ~Snapshot() = default;
    virtual bool Init() = 0;
    virtual int MakeSnapshot(std::shared_ptr<Table> table,
                             uint64_t& out_offset,  // NOLINT
                             uint64_t end_offset,
                             uint64_t term = 0) = 0;
    virtual bool Recover(std::shared_ptr<Table> table,
                         uint64_t& latest_offset) = 0;  // NOLINT
    uint64_t GetOffset() { return offset_; }
    int GenManifest(const std::string& snapshot_name, uint64_t key_count, uint64_t offset, uint64_t term);
    static int GetLocalManifest(const std::string& full_path,
                                ::openmldb::api::Manifest& manifest);  // NOLINT
    std::string GetSnapshotPath() { return snapshot_path_; }
 protected:
    uint32_t tid_;
    uint32_t pid_;
    uint64_t offset_;
    std::atomic<bool> making_snapshot_;
    std::string snapshot_path_;
};

}  // namespace storage
}  // namespace openmldb
