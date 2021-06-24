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

using ::openmldb::api::LogEntry;
using ::openmldb::base::Slice;

namespace openmldb {
namespace storage {

typedef google::protobuf::RepeatedPtrField<::openmldb::api::Dimension> Dimensions;

class MemTableWindowIterator : public ::hybridse::vm::RowIterator {
 public:
    MemTableWindowIterator(TimeEntries::Iterator* it, ::openmldb::storage::TTLType ttl_type, uint64_t expire_time,
                           uint64_t expire_cnt)
        : it_(it), record_idx_(0), expire_value_(expire_time, expire_cnt, ttl_type), row_() {}

    ~MemTableWindowIterator() { delete it_; }

    inline bool Valid() const {
        if (!it_->Valid() || expire_value_.IsExpired(it_->GetKey(), record_idx_)) {
            return false;
        }
        return true;
    }

    inline void Next() {
        it_->Next();
        record_idx_++;
    }

    inline const uint64_t& GetKey() const { return it_->GetKey(); }

    // TODO(wangtaize) unify the row object
    inline const ::hybridse::codec::Row& GetValue() {
        row_.Reset(reinterpret_cast<const int8_t*>(it_->GetValue()->data), it_->GetValue()->size);
        return row_;
    }
    inline void Seek(const uint64_t& key) { it_->Seek(key); }
    inline void SeekToFirst() { it_->SeekToFirst(); }
    inline bool IsSeekable() const { return true; }

 private:
    TimeEntries::Iterator* it_;
    uint32_t record_idx_;
    TTLSt expire_value_;
    ::hybridse::codec::Row row_;
};

class MemTableKeyIterator : public ::hybridse::vm::WindowIterator {
 public:
    MemTableKeyIterator(Segment** segments, uint32_t seg_cnt, ::openmldb::storage::TTLType ttl_type,
                        uint64_t expire_time, uint64_t expire_cnt, uint32_t ts_index);

    ~MemTableKeyIterator();

    void Seek(const std::string& key);

    void SeekToFirst();

    void Next();

    bool Valid();

    std::unique_ptr<::hybridse::vm::RowIterator> GetValue();
    ::hybridse::vm::RowIterator* GetRawValue();

    const hybridse::codec::Row GetKey();

 private:
    void NextPK();

 private:
    Segment** segments_;
    uint32_t const seg_cnt_;
    uint32_t seg_idx_;
    KeyEntries::Iterator* pk_it_;
    TimeEntries::Iterator* it_;
    ::openmldb::storage::TTLType ttl_type_;
    uint64_t expire_time_;
    uint64_t expire_cnt_;
    uint32_t ts_index_;
    Ticket ticket_;
    uint32_t ts_idx_;
};

class MemTableTraverseIterator : public TableIterator {
 public:
    MemTableTraverseIterator(Segment** segments, uint32_t seg_cnt, ::openmldb::storage::TTLType ttl_type,
                             uint64_t expire_time, uint64_t expire_cnt, uint32_t ts_index);
    virtual ~MemTableTraverseIterator();
    inline bool Valid() override;
    void Next() override;
    void Seek(const std::string& key, uint64_t time) override;
    openmldb::base::Slice GetValue() const override;
    std::string GetPK() const override;
    uint64_t GetKey() const override;
    void SeekToFirst() override;
    uint64_t GetCount() const override;

 private:
    void NextPK();

 private:
    Segment** segments_;
    uint32_t const seg_cnt_;
    uint32_t seg_idx_;
    KeyEntries::Iterator* pk_it_;
    TimeEntries::Iterator* it_;
    uint32_t record_idx_;
    uint32_t ts_idx_;
    // uint64_t expire_value_;
    TTLSt expire_value_;
    Ticket ticket_;
    uint64_t traverse_cnt_;
};

class MemTable : public Table {
 public:
    MemTable(const std::string& name, uint32_t id, uint32_t pid, uint32_t seg_cnt,
             const std::map<std::string, uint32_t>& mapping, uint64_t ttl, ::openmldb::type::TTLType ttl_type);

    explicit MemTable(const ::openmldb::api::TableMeta& table_meta);
    virtual ~MemTable();
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    bool Init() override;

    // Put a record
    bool Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) override;

    // Put a multi dimension record
    bool Put(uint64_t time, const std::string& value, const Dimensions& dimensions) override;

    // Note the method should incr record_cnt_ manually
    bool Put(const Slice& pk, uint64_t time, DataBlock* row, uint32_t idx);

    bool Put(const Dimensions& dimensions, const TSDimensions& ts_dimemsions, const std::string& value) override;

    bool Delete(const std::string& pk, uint32_t idx) override;

    // use the first demission
    TableIterator* NewIterator(const std::string& pk, Ticket& ticket) override;

    TableIterator* NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) override;

    TableIterator* NewTraverseIterator(uint32_t index) override;

    ::hybridse::vm::WindowIterator* NewWindowIterator(uint32_t index);

    // release all memory allocated
    uint64_t Release();

    void SchedGc() override;

    int GetCount(uint32_t index, const std::string& pk,
                 uint64_t& count);  // NOLINT

    uint64_t GetRecordIdxCnt();
    bool GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size);
    uint64_t GetRecordIdxByteSize();
    uint64_t GetRecordPkCnt();

    void SetCompressType(::openmldb::type::CompressType compress_type);
    ::openmldb::type::CompressType GetCompressType();

    inline uint64_t GetRecordByteSize() const { return record_byte_size_.load(std::memory_order_relaxed); }

    uint64_t GetRecordCnt() const override { return record_cnt_.load(std::memory_order_relaxed); }

    inline uint32_t GetSegCnt() const { return seg_cnt_; }

    inline void SetExpire(bool is_expire) { enable_gc_.store(is_expire, std::memory_order_relaxed); }

    uint64_t GetExpireTime(const TTLSt& ttl_st) override;

    bool IsExpire(const ::openmldb::api::LogEntry& entry) override;

    inline bool GetExpireStatus() { return enable_gc_.load(std::memory_order_relaxed); }

    inline void RecordCntIncr() { record_cnt_.fetch_add(1, std::memory_order_relaxed); }

    inline void RecordCntIncr(uint32_t cnt) { record_cnt_.fetch_add(cnt, std::memory_order_relaxed); }

    inline uint32_t GetKeyEntryHeight() { return key_entry_max_height_; }

    bool DeleteIndex(std::string idx_name);

    bool AddIndex(const ::openmldb::common::ColumnKey& column_key);

 private:
    bool CheckAbsolute(const TTLSt& ttl, uint64_t ts);

    bool CheckLatest(uint32_t index_id, const std::string& key, uint64_t ts);

 private:
    uint32_t seg_cnt_;
    std::vector<Segment**> segments_;
    std::atomic<bool> enable_gc_;
    uint64_t ttl_offset_;
    std::atomic<uint64_t> record_cnt_;
    bool segment_released_;
    std::atomic<uint64_t> record_byte_size_;
    uint32_t key_entry_max_height_;
};

}  // namespace storage
}  // namespace openmldb

#endif  // SRC_STORAGE_MEM_TABLE_H_
