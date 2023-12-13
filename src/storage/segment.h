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

#ifndef SRC_STORAGE_SEGMENT_H_
#define SRC_STORAGE_SEGMENT_H_

#include <atomic>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <string>
#include <vector>

#include "base/skiplist.h"
#include "base/slice.h"
#include "proto/tablet.pb.h"
#include "storage/iterator.h"
#include "storage/key_entry.h"
#include "storage/node_cache.h"
#include "storage/schema.h"
#include "storage/ticket.h"

namespace openmldb {
namespace storage {

using ::openmldb::base::Slice;

class MemTableIterator : public TableIterator {
 public:
    explicit MemTableIterator(TimeEntries::Iterator* it, type::CompressType compress_type);
    virtual ~MemTableIterator();
    void Seek(const uint64_t time) override;
    bool Valid() override;
    void Next() override;
    openmldb::base::Slice GetValue() const override;
    uint64_t GetKey() const override;
    void SeekToFirst() override;
    void SeekToLast() override;

 private:
    TimeEntries::Iterator* it_;
    type::CompressType compress_type_;
    mutable std::string tmp_buf_;
};

struct SliceComparator {
    int operator()(const ::openmldb::base::Slice& a, const ::openmldb::base::Slice& b) const { return a.compare(b); }
};

using KeyEntries = base::Skiplist<base::Slice, void*, SliceComparator>;
using KeyEntryNodeList = base::Skiplist<uint64_t, base::Node<Slice, void*>*, TimeComparator>;

class Segment {
 public:
    explicit Segment(uint8_t height);
    Segment(uint8_t height, const std::vector<uint32_t>& ts_idx_vec);
    ~Segment();

    // legacy interface called by memtable and ut
    void Put(const Slice& key, uint64_t time, const char* data, uint32_t size, bool put_if_absent = false, bool check_all_time = false);

    bool Put(const Slice& key, uint64_t time, DataBlock* row, bool put_if_absent = false, bool check_all_time = false);

    void BulkLoadPut(unsigned int key_entry_id, const Slice& key, uint64_t time, DataBlock* row);
    // main put method
    bool Put(const Slice& key, const std::map<int32_t, uint64_t>& ts_map, DataBlock* row, bool put_if_absent = false);

    bool Delete(const std::optional<uint32_t>& idx, const Slice& key);
    bool Delete(const std::optional<uint32_t>& idx, const Slice& key, uint64_t ts,
                const std::optional<uint64_t>& end_ts);

    void Release(StatisticsInfo* statistics_info);

    void ExecuteGc(const TTLSt& ttl_st, StatisticsInfo* statistics_info);
    void ExecuteGc(const std::map<uint32_t, TTLSt>& ttl_st_map, StatisticsInfo* statistics_info);

    void Gc4TTL(const uint64_t time, StatisticsInfo* statistics_info);
    void Gc4Head(uint64_t keep_cnt, StatisticsInfo* statistics_info);
    void Gc4TTLAndHead(const uint64_t time, const uint64_t keep_cnt, StatisticsInfo* statistics_info);
    void Gc4TTLOrHead(const uint64_t time, const uint64_t keep_cnt, StatisticsInfo* statistics_info);
    void GcAllType(const std::map<uint32_t, TTLSt>& ttl_st_map, StatisticsInfo* statistics_info);

    MemTableIterator* NewIterator(const Slice& key, Ticket& ticket, type::CompressType compress_type);  // NOLINT
    MemTableIterator* NewIterator(const Slice& key, uint32_t idx, Ticket& ticket,
                                  type::CompressType compress_type);  // NOLINT

    uint64_t GetIdxCnt() const { return idx_cnt_vec_[0]->load(std::memory_order_relaxed); }

    int GetIdxCnt(uint32_t ts_idx, uint64_t& ts_cnt) {  // NOLINT
        uint32_t real_idx = 0;
        if (GetTsIdx(ts_idx, real_idx) < 0) {
            return -1;
        }
        ts_cnt = idx_cnt_vec_[real_idx]->load(std::memory_order_relaxed);
        return 0;
    }

    inline uint64_t GetTsCnt() { return ts_cnt_; }

    int GetTsIdx(uint32_t raw_idx, uint32_t& real_idx) {  // NOLINT
        auto iter = ts_idx_map_.find(raw_idx);
        if (iter == ts_idx_map_.end()) {
            return -1;
        } else {
            real_idx = iter->second;
        }
        return 0;
    }

    const std::map<uint32_t, uint32_t>& GetTsIdxMap() const { return ts_idx_map_; }

    inline uint64_t GetIdxByteSize() { return idx_byte_size_.load(std::memory_order_relaxed); }

    inline uint64_t GetPkCnt() { return pk_cnt_.load(std::memory_order_relaxed); }

    void GcFreeList(StatisticsInfo* statistics_info);

    KeyEntries* GetKeyEntries() { return entries_; }

    int GetCount(const Slice& key, uint64_t& count);                // NOLINT
    int GetCount(const Slice& key, uint32_t idx, uint64_t& count);  // NOLINT

    void IncrGcVersion() { gc_version_.fetch_add(1, std::memory_order_relaxed); }

    void ReleaseAndCount(StatisticsInfo* statistics_info);

    void ReleaseAndCount(const std::vector<size_t>& id_vec, StatisticsInfo* statistics_info);

 private:
    void FreeList(uint32_t ts_idx, ::openmldb::base::Node<uint64_t, DataBlock*>* node, StatisticsInfo* statistics_info);
    void SplitList(KeyEntry* entry, uint64_t ts, ::openmldb::base::Node<uint64_t, DataBlock*>** node);

    bool ListContains(KeyEntry* entry, uint64_t time, DataBlock* row, bool check_all_time);

    bool PutUnlock(const Slice& key, uint64_t time, DataBlock* row, bool put_if_absent = false, bool check_all_time = false);

 private:
    KeyEntries* entries_;
    std::mutex mu_;
    std::atomic<uint64_t> idx_byte_size_;
    std::atomic<uint64_t> pk_cnt_;
    uint8_t key_entry_max_height_;
    uint32_t ts_cnt_;
    std::atomic<uint64_t> gc_version_;
    std::map<uint32_t, uint32_t> ts_idx_map_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>> idx_cnt_vec_;
    uint64_t ttl_offset_;
    NodeCache node_cache_;
};

}  // namespace storage
}  // namespace openmldb
#endif  // SRC_STORAGE_SEGMENT_H_
