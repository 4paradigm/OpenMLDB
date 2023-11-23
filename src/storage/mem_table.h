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

#ifndef SRC_STORAGE_MEM_TABLE_H_
#define SRC_STORAGE_MEM_TABLE_H_

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "proto/tablet.pb.h"
#include "storage/iterator.h"
#include "storage/segment.h"
#include "storage/table.h"
#include "storage/ticket.h"
#include "vm/catalog.h"

namespace openmldb {
namespace storage {

typedef google::protobuf::RepeatedPtrField<::openmldb::api::Dimension> Dimensions;
using ::openmldb::api::LogEntry;
using ::openmldb::base::Slice;

class MemTable : public Table {
 public:
    MemTable(const std::string& name, uint32_t id, uint32_t pid, uint32_t seg_cnt,
             const std::map<std::string, uint32_t>& mapping, uint64_t ttl, ::openmldb::type::TTLType ttl_type);

    explicit MemTable(const ::openmldb::api::TableMeta& table_meta);
    virtual ~MemTable();
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    bool Init() override;

    bool Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) override;

    bool Put(uint64_t time, const std::string& value, const Dimensions& dimensions) override;

    bool GetBulkLoadInfo(::openmldb::api::BulkLoadInfoResponse* response);

    bool BulkLoad(const std::vector<DataBlock*>& data_blocks,
                  const ::google::protobuf::RepeatedPtrField<::openmldb::api::BulkLoadIndex>& indexes);

    bool Delete(const ::openmldb::api::LogEntry& entry) override;
    bool Delete(uint32_t idx, const std::string& key,
            const std::optional<uint64_t>& start_ts, const std::optional<uint64_t>& end_ts);

    // use the first demission
    TableIterator* NewIterator(const std::string& pk, Ticket& ticket) override;

    TableIterator* NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) override;

    TraverseIterator* NewTraverseIterator(uint32_t index) override;

    ::hybridse::vm::WindowIterator* NewWindowIterator(uint32_t index);

    // release all memory allocated
    uint64_t Release();

    void SchedGc() override;

    int GetCount(uint32_t index, const std::string& pk, uint64_t& count) override;  // NOLINT

    uint64_t GetRecordIdxCnt() override;
    bool GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size) override;
    uint64_t GetRecordIdxByteSize() override;
    uint64_t GetRecordPkCnt() override;

    void SetCompressType(::openmldb::type::CompressType compress_type);
    ::openmldb::type::CompressType GetCompressType();

    uint64_t GetRecordByteSize() const override { return record_byte_size_.load(std::memory_order_relaxed); }

    uint64_t GetRecordCnt() override { return GetRecordIdxCnt(); }

    inline uint32_t GetSegCnt() const { return seg_cnt_; }

    inline void SetExpire(bool is_expire) { enable_gc_.store(is_expire, std::memory_order_relaxed); }

    uint64_t GetExpireTime(const TTLSt& ttl_st) override;

    bool IsExpire(const ::openmldb::api::LogEntry& entry) override;

    inline bool GetExpireStatus() { return enable_gc_.load(std::memory_order_relaxed); }

    inline uint32_t GetKeyEntryHeight() const { return key_entry_max_height_; }

    bool DeleteIndex(const std::string& idx_name) override;

    bool AddIndex(const ::openmldb::common::ColumnKey& column_key);

 private:
    bool CheckAbsolute(const TTLSt& ttl, uint64_t ts);

    bool CheckLatest(uint32_t index_id, const std::string& key, uint64_t ts);

 private:
    uint32_t seg_cnt_;
    std::vector<Segment**> segments_;
    std::atomic<bool> enable_gc_;
    uint64_t ttl_offset_;
    bool segment_released_;
    std::atomic<uint64_t> record_byte_size_;
    uint32_t key_entry_max_height_;
};

}  // namespace storage
}  // namespace openmldb

#endif  // SRC_STORAGE_MEM_TABLE_H_
