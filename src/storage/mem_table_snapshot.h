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

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "base/status.h"
#include "codec/schema_codec.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "log/sequential_file.h"
#include "proto/tablet.pb.h"
#include "storage/snapshot.h"
#include "storage/table.h"

using ::openmldb::api::LogEntry;
namespace openmldb {
namespace storage {

using ::openmldb::log::WriteHandle;

typedef ::openmldb::base::Skiplist<uint32_t, uint64_t, ::openmldb::base::DefaultComparator> LogParts;

// table snapshot
class MemTableSnapshot : public Snapshot {
 public:
    MemTableSnapshot(uint32_t tid, uint32_t pid, LogParts* log_part, const std::string& db_root_path);

    virtual ~MemTableSnapshot() = default;

    bool Init() override;

    bool Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) override;

    void RecoverFromSnapshot(const std::string& snapshot_name, uint64_t expect_cnt, std::shared_ptr<Table> table);

    int MakeSnapshot(std::shared_ptr<Table> table,
                     uint64_t& out_offset,  // NOLINT
                     uint64_t end_offset,
                     uint64_t term = 0) override;

    int TTLSnapshot(std::shared_ptr<Table> table, const ::openmldb::api::Manifest& manifest, WriteHandle* wh,
                    uint64_t& count, uint64_t& expired_key_num,  // NOLINT
                    uint64_t& deleted_key_num);                  // NOLINT

    void Put(std::string& path, std::shared_ptr<Table>& table,  // NOLINT
             std::vector<std::string*> recordPtr, std::atomic<uint64_t>* succ_cnt, std::atomic<uint64_t>* failed_cnt);

    std::string GenSnapshotName();

    base::Status GetAllDecoder(std::shared_ptr<Table> table, std::map<uint8_t, codec::RowView>* decoder_map);

    base::Status GetIndexKey(std::shared_ptr<Table> table, const std::shared_ptr<IndexDef>& index,
            const base::Slice& data, std::map<uint8_t, codec::RowView>* decoder_map, std::string* index_key);

    base::Status ExtractIndexFromSnapshot(std::shared_ptr<Table> table, const ::openmldb::api::Manifest& manifest,
            WriteHandle* wh, const std::vector<::openmldb::common::ColumnKey>& add_indexs,
            uint32_t partition_num, uint64_t* count, uint64_t* expired_key_num, uint64_t* deleted_key_num);

    int CheckDeleteAndUpdate(std::shared_ptr<Table> table, ::openmldb::api::LogEntry* new_entry);

    base::Status ExtractIndexFromBinlog(std::shared_ptr<Table> table,
            WriteHandle* wh, const std::vector<::openmldb::common::ColumnKey>& add_indexs,
            uint64_t collected_offset, uint32_t partition_num, uint64_t* offset,
            uint64_t* last_term, uint64_t* count, uint64_t* expired_key_num, uint64_t* deleted_key_num);

    int ExtractIndexFromSnapshot(std::shared_ptr<Table> table, const ::openmldb::api::Manifest& manifest,
                                 WriteHandle* wh,
                                 const ::openmldb::common::ColumnKey& column_key,  // NOLINT
                                 uint32_t idx, uint32_t partition_num, uint32_t max_idx,
                                 const std::vector<uint32_t>& index_cols,
                                 uint64_t& count,                                        // NOLINT
                                 uint64_t& expired_key_num, uint64_t& deleted_key_num);  // NOLINT

    bool DumpSnapshotIndexData(std::shared_ptr<Table> table, const std::vector<std::vector<uint32_t>>& index_cols,
                               uint32_t max_idx, uint32_t idx, const std::vector<::openmldb::log::WriteHandle*>& whs,
                               uint64_t* snapshot_offset);

    bool DumpBinlogIndexData(std::shared_ptr<Table> table, const std::vector<std::vector<uint32_t>>& index_cols,
                             uint32_t max_idx, uint32_t idx, const std::vector<::openmldb::log::WriteHandle*>& whs,
                             uint64_t snapshot_offset, uint64_t collected_offset);

    int ExtractIndexData(std::shared_ptr<Table> table, const ::openmldb::common::ColumnKey& column_key, uint32_t idx,
                         uint32_t partition_num,
                         uint64_t& out_offset);  // NOLINT

    int ExtractIndexData(std::shared_ptr<Table> table, const std::vector<::openmldb::common::ColumnKey>& column_key,
                        uint32_t partition_num, uint64_t* out_offset);

    bool DumpIndexData(std::shared_ptr<Table> table, const ::openmldb::common::ColumnKey& column_key, uint32_t idx,
                       const std::vector<::openmldb::log::WriteHandle*>& whs);

    bool PackNewIndexEntry(std::shared_ptr<Table> table, const std::vector<std::vector<uint32_t>>& index_cols,
                           uint32_t max_idx, uint32_t idx, uint32_t partition_num, ::openmldb::api::LogEntry* entry,
                           uint32_t* index_pid);

    int RemoveDeletedKey(std::shared_ptr<Table> table, const ::openmldb::api::LogEntry& entry,
                         const std::set<uint32_t>& deleted_index,
                         std::string* buffer);

 private:
    // load single snapshot to table
    void RecoverSingleSnapshot(const std::string& path, std::shared_ptr<Table> table, std::atomic<uint64_t>* g_succ_cnt,
                               std::atomic<uint64_t>* g_failed_cnt);

    uint64_t CollectDeletedKey(std::shared_ptr<Table> table, uint64_t end_offset);

    int DecodeData(std::shared_ptr<Table> table, const openmldb::api::LogEntry& entry, uint32_t maxIdx,
                   std::vector<std::string>& row);  // NOLINT

    inline bool IsCompressed(const std::string& path);

 private:
    LogParts* log_part_;
    std::string log_path_;
    std::map<std::string, uint64_t> deleted_keys_;
    std::string db_root_path_;
};

}  // namespace storage
}  // namespace openmldb
