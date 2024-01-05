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

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "base/status.h"
#include "codec/schema_codec.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "log/sequential_file.h"
#include "proto/tablet.pb.h"
#include "storage/snapshot.h"

namespace openmldb {
namespace storage {

using ::openmldb::log::WriteHandle;
typedef ::openmldb::base::Skiplist<uint32_t, uint64_t, ::openmldb::base::DefaultComparator> LogParts;

struct MemSnapshotMeta : SnapshotMeta {
    MemSnapshotMeta(const std::string& name, const std::string& snapshot_path,
            const std::string& compression) : SnapshotMeta(name), snapshot_compression(compression) {
        if (snapshot_compression != "off") {
            snapshot_name.append(".");
            snapshot_name.append(snapshot_compression);
        }
        snapshot_name_tmp = snapshot_name + ".tmp";
        full_path = snapshot_path + snapshot_name;
        tmp_file_path = snapshot_path + snapshot_name_tmp;
    }

    uint64_t expired_key_num = 0;
    uint64_t deleted_key_num = 0;
    std::string snapshot_compression;
    std::string snapshot_name_tmp;
    std::string full_path;
    std::string tmp_file_path;
};

enum class DataReaderType {
    kSnapshot = 1,
    kBinlog = 2,
    kSnapshotAndBinlog = 3
};

class DataReader {
 public:
    DataReader(const std::string& snapshot_path, LogParts* log_part,
            const std::string& log_path, DataReaderType type, uint64_t start_offset, uint64_t end_offset)
        : snapshot_path_(snapshot_path), log_part_(log_part), log_path_(log_path), read_type_(type),
        start_offset_(start_offset), end_offset_(end_offset) {}
    DataReader(const DataReader&) = delete;
    DataReader& operator=(const DataReader&) = delete;

    static std::shared_ptr<DataReader> CreateDataReader(const std::string& snapshot_path, LogParts* log_part,
            const std::string& log_path, DataReaderType type);
    static std::shared_ptr<DataReader> CreateDataReader(const std::string& snapshot_path, LogParts* log_part,
            const std::string& log_path, DataReaderType type, uint64_t end_offset);
    static std::shared_ptr<DataReader> CreateDataReader(LogParts* log_part, const std::string& log_path,
            uint64_t start_offset, uint64_t end_offset);

    bool HasNext();
    ::openmldb::api::LogEntry& GetValue() { return entry_; }
    const std::string& GetStrValue() { return entry_buff_; }
    bool Init();

 private:
    bool ReadFromSnapshot();
    bool ReadFromBinlog();

 private:
    std::string snapshot_path_;
    LogParts* log_part_;
    std::string log_path_;
    DataReaderType read_type_;
    uint64_t start_offset_ = 0;
    uint64_t end_offset_ = 0;
    uint64_t cur_offset_ = 0;
    bool read_snapshot_ = false;
    bool read_binlog_ = false;
    std::shared_ptr<::openmldb::log::SequentialFile> seq_file_;
    std::shared_ptr<::openmldb::log::Reader> snapshot_reader_;
    std::shared_ptr<::openmldb::log::LogReader> binlog_reader_;
    std::string buffer_;
    std::string entry_buff_;
    ::openmldb::base::Slice record_;
    ::openmldb::api::LogEntry entry_;
    uint64_t succ_cnt_ = 0;
    uint64_t failed_cnt_ = 0;
};

class TableIndexInfo {
 public:
    TableIndexInfo(const ::openmldb::api::TableMeta& table_meta,
             const std::vector<::openmldb::common::ColumnKey>& add_indexs)
        : table_meta_(table_meta), add_indexs_(add_indexs) {}
    bool Init();
    const std::vector<uint32_t>& GetAllIndexCols() const { return all_index_cols_; }
    const std::vector<uint32_t>& GetAddIndexIdx() const { return add_index_idx_vec_; }
    bool HasIndex(uint32_t idx) const;
    const std::vector<uint32_t>& GetIndexCols(uint32_t idx);
    const std::vector<uint32_t>& GetRealIndexCols(uint32_t idx);  // the pos in all_index_cols_

 private:
    ::openmldb::api::TableMeta table_meta_;
    std::vector<::openmldb::common::ColumnKey> add_indexs_;
    std::map<std::string, uint32_t> column_idx_map_;
    std::vector<uint32_t> all_index_cols_;
    std::vector<uint32_t> add_index_idx_vec_;
    std::map<uint32_t, std::vector<uint32_t>> index_cols_map_;
    std::map<uint32_t, std::vector<uint32_t>> real_index_cols_map_;
};

struct DeleteSpan {
    DeleteSpan() = default;
    explicit DeleteSpan(const api::LogEntry& entry);
    bool IsDeleted(uint64_t offset_i, uint32_t idx_i, uint64_t ts) const;

    uint64_t offset = 0;
    std::optional<uint32_t> idx = std::nullopt;
    uint64_t start_ts = UINT64_MAX;
    std::optional<uint64_t> end_ts = std::nullopt;
};

class DeleteCollector {
 public:
    bool IsDeleted(uint64_t offset, uint32_t idx, const std::string& key, uint64_t ts) const;
    bool IsEmpty() const;
    void Clear();
    void AddSpan(uint64_t offset, DeleteSpan span);
    void AddSpan(std::string key, DeleteSpan span);
    void AddKey(uint64_t offset, std::string key);
    size_t Size() const;

 private:
    absl::flat_hash_map<std::string, uint64_t> deleted_keys_;
    absl::flat_hash_map<std::string, DeleteSpan> deleted_spans_;
    absl::btree_map<uint64_t, DeleteSpan> no_key_spans_;
};

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

    int TTLSnapshot(std::shared_ptr<Table> table, const ::openmldb::api::Manifest& manifest,
            const std::shared_ptr<WriteHandle>& wh, MemSnapshotMeta* snapshot_meta);

    void Put(std::string& path, std::shared_ptr<Table>& table,  // NOLINT
             std::vector<std::string*> recordPtr, std::atomic<uint64_t>* succ_cnt, std::atomic<uint64_t>* failed_cnt);

    base::Status ExtractIndexData(const std::shared_ptr<Table>& table,
            const std::vector<::openmldb::common::ColumnKey>& add_indexs,
            const std::vector<std::shared_ptr<::openmldb::log::WriteHandle>>& whs,
            uint64_t offset, bool dump_data);

    int CheckDeleteAndUpdate(std::shared_ptr<Table> table, ::openmldb::api::LogEntry* new_entry);

    int Truncate(uint64_t offset, uint64_t term);

 private:
    // load single snapshot to table
    void RecoverSingleSnapshot(const std::string& path, std::shared_ptr<Table> table, std::atomic<uint64_t>* g_succ_cnt,
                               std::atomic<uint64_t>* g_failed_cnt);

    uint64_t CollectDeletedKey(uint64_t end_offset);

    ::openmldb::base::Status DecodeData(const std::shared_ptr<Table>& table, const openmldb::api::LogEntry& entry,
            const std::vector<uint32_t>& cols, std::vector<std::string>* row);

    std::string GenSnapshotName();

    ::openmldb::base::Status WriteSnapshot(const MemSnapshotMeta& snapshot_meta);

 private:
    LogParts* log_part_;
    std::string log_path_;
    std::string db_root_path_;
    DeleteCollector delete_collector_;
};

}  // namespace storage
}  // namespace openmldb
