//
// table.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31 
// 


#ifndef RTIDB_STORAGE_TABLE_H
#define RTIDB_STORAGE_TABLE_H

#include <vector>
#include <map>
#include "storage/segment.h"
#include "storage/ticket.h"
#include "storage/iterator.h"
#include "storage/table.h"
#include <atomic>
#include <memory>
#include "proto/tablet.pb.h"

using ::rtidb::base::Slice;
using ::rtidb::api::LogEntry;

namespace rtidb {
namespace storage {

typedef google::protobuf::RepeatedPtrField<::rtidb::api::Dimension> Dimensions;

class MemTableTraverseIterator : public TableIterator {
public:
    MemTableTraverseIterator(Segment** segments, uint32_t seg_cnt, ::rtidb::common::TTLType ttl_type, const uint64_t& expire_time, const uint64_t& expire_cnt, uint32_t ts_index);
    ~MemTableTraverseIterator();
    virtual bool Valid() override;
    virtual void Next() override;
    virtual void Seek(const std::string& key, uint64_t time) override;
    virtual rtidb::base::Slice GetValue() const override;
    virtual std::string GetPK() const override;
    virtual uint64_t GetKey() const override;
    virtual void SeekToFirst() override;
    virtual uint64_t GetCount() const override;

private:
    void NextPK();
    bool IsExpired();

private:
    Segment** segments_;
    uint32_t const seg_cnt_;
    uint32_t seg_idx_;
    KeyEntries::Iterator* pk_it_;
    TimeEntries::Iterator* it_;
    ::rtidb::common::TTLType ttl_type_;
    uint32_t record_idx_;
    uint32_t ts_idx_;
    // uint64_t expire_value_;
    TTLDesc expire_value_;
    Ticket ticket_;
    uint64_t traverse_cnt_;
};

class MemTable : public Table {

public:

    MemTable(const std::string& name,
          uint32_t id,
          uint32_t pid,
          uint32_t seg_cnt,
          const std::map<std::string, uint32_t>& mapping, uint64_t ttl);

    MemTable(const ::rtidb::api::TableMeta& table_meta);
    virtual ~MemTable();
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    virtual bool Init() override;

    // Put a record
    virtual bool Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) override;

    // Put a multi dimension record
    virtual bool Put(uint64_t time, const std::string& value, const Dimensions& dimensions) override;

    // Note the method should incr record_cnt_ manually
    bool Put(const Slice& pk, uint64_t time, DataBlock* row, uint32_t idx);

    virtual bool Put(const Dimensions& dimensions, const TSDimensions& ts_dimemsions, const std::string& value) override;

    virtual bool Delete(const std::string& pk, uint32_t idx) override;

    // use the first demission
    virtual TableIterator* NewIterator(const std::string& pk, Ticket& ticket) override;

    virtual TableIterator* NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) override;

    virtual TableIterator* NewIterator(uint32_t index, int32_t ts_idx, const std::string& pk, Ticket& ticket);

    virtual TableIterator* NewTraverseIterator(uint32_t index) override;
    virtual TableIterator* NewTraverseIterator(uint32_t index, uint32_t ts_idx) override;
    // release all memory allocated
    uint64_t Release();

    virtual void SchedGc() override;

    int GetCount(uint32_t index, const std::string& pk, uint64_t& count);
    int GetCount(uint32_t index, uint32_t ts_idx, const std::string& pk, uint64_t& count);

    uint64_t GetRecordIdxCnt();
    bool GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size);
    uint64_t GetRecordIdxByteSize();
    uint64_t GetRecordPkCnt();

    void SetCompressType(::rtidb::api::CompressType compress_type);
    ::rtidb::api::CompressType GetCompressType();

    inline uint64_t GetRecordByteSize() const {
        return record_byte_size_.load(std::memory_order_relaxed);    
    }

    virtual uint64_t GetRecordCnt() const override {
        return record_cnt_.load(std::memory_order_relaxed);
    }

    inline uint32_t GetSegCnt() const {
        return seg_cnt_;
    }

    inline void SetExpire(bool is_expire) {
        enable_gc_.store(is_expire, std::memory_order_relaxed);
    }

    virtual uint64_t GetExpireTime(uint64_t ttl) override;

    virtual bool IsExpire(const ::rtidb::api::LogEntry& entry) override;

    inline bool GetExpireStatus() {
        return enable_gc_.load(std::memory_order_relaxed);
    }

    inline void SetTimeOffset(int64_t offset) {
        time_offset_.store(offset * 1000, std::memory_order_relaxed); // convert to millisecond
    }

    inline int64_t GetTimeOffset() {
       return  time_offset_.load(std::memory_order_relaxed) / 1000;
    }

    inline void RecordCntIncr() {
        record_cnt_.fetch_add(1, std::memory_order_relaxed);
    }

    inline void RecordCntIncr(uint32_t cnt) {
        record_cnt_.fetch_add(cnt, std::memory_order_relaxed);
    }

    inline void SetTTL(const TTLDesc& ttl) {
        new_abs_ttl_.store(ttl.abs_ttl * 60 * 1000);
        new_lat_ttl_.store(ttl.lat_ttl);
    }

    inline void SetTTL(const uint32_t ts_idx, const TTLDesc& ttl) {
        if (ts_idx < new_abs_ttl_vec_.size()) {
            new_abs_ttl_vec_[ts_idx]->store(ttl.abs_ttl * 60 * 1000, std::memory_order_relaxed);
        }
        if (ts_idx < new_lat_ttl_vec_.size()) {
            new_lat_ttl_vec_[ts_idx]->store(ttl.abs_ttl, std::memory_order_relaxed);
        }
    }

    inline uint32_t GetKeyEntryHeight() {
        return key_entry_max_height_;
    }

private:
    uint32_t seg_cnt_;
    Segment*** segments_;
    std::atomic<bool> enable_gc_;
    uint64_t ttl_offset_;
    std::atomic<uint64_t> record_cnt_;
    std::atomic<int64_t> time_offset_;
    bool segment_released_;
    std::atomic<uint64_t> record_byte_size_;
    uint32_t key_entry_max_height_;
};

}
}

#endif /* !TABLE_H */
