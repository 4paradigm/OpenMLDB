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
#include <atomic>
#include "proto/tablet.pb.h"

using ::rtidb::base::Slice;
using ::rtidb::api::LogEntry;

namespace rtidb {
namespace storage {

enum TableGcType {
    kTTL,
    kCountLimit
};

enum TableStat {
    kUndefined = 0,
    kNormal,
    kLoading,
    kMakingSnapshot,
    kSnapshotPaused
};

typedef google::protobuf::RepeatedPtrField<::rtidb::api::Dimension> Dimensions;

class Table {

public:

    // Create a logic table with table name , table id, table partition id 
    // and segment count
    Table(const std::string& name,
          uint32_t id,
          uint32_t pid,
          uint32_t seg_cnt,
          const std::map<std::string, uint32_t>& mapping,
          uint64_t ttl,
          bool is_leader,
          const std::vector<std::string>& replicas,
          uint32_t key_entry_max_height);

    Table(const std::string& name,
          uint32_t id,
          uint32_t pid,
          uint32_t seg_cnt,
          const std::map<std::string, uint32_t>& mapping,
          uint64_t ttl);

    ~Table();

    void Init();

    void SetGcSafeOffset(uint64_t offset);

    // Put a record
    bool Put(const std::string& pk,
             uint64_t time,
             const char* data,
             uint32_t size);

    // Put a multi dimension record
    bool Put(uint64_t time, 
             const std::string& value,
             const Dimensions& dimensions);

    // Note the method should incr record_cnt_ manually
    bool Put(const Slice& pk, uint64_t time, DataBlock* row, uint32_t idx);

    bool Delete(const std::string& pk, uint32_t idx);

    // use the first demission
    Iterator* NewIterator(const std::string& pk);

    Iterator* NewIterator(uint32_t index, const std::string& pk);
    // release all memory allocated
    uint64_t Release();

    uint64_t SchedGc();

    uint64_t GetTTL() const {
        return ttl_.load(std::memory_order_relaxed) / (60 * 1000);
    }

    uint64_t GetRecordIdxCnt();
    bool GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size);
    uint64_t GetRecordIdxByteSize();
    uint64_t GetRecordPkCnt();

    void SetCompressType(::rtidb::api::CompressType compress_type);
    ::rtidb::api::CompressType GetCompressType();

    inline uint64_t GetRecordByteSize() const {
        return record_byte_size_.load(std::memory_order_relaxed);    
    }

    inline uint64_t GetRecordCnt() const {
        return record_cnt_.load(std::memory_order_relaxed);
    }


    inline std::string GetName() const {
        return name_;
    }

    inline uint32_t GetId() const {
        return id_;
    }

    inline uint32_t GetSegCnt() const {
        return seg_cnt_;
    }

    inline uint32_t GetIdxCnt() const {
        return idx_cnt_;
    }

    inline uint32_t GetPid() const {
        return pid_;
    }

    inline bool IsLeader() const {
        return is_leader_;
    }

    void SetLeader(bool is_leader) {
        is_leader_ = is_leader;
    }

    inline const std::vector<std::string>& GetReplicas() const {
        return replicas_;
    }

    void SetReplicas(const std::vector<std::string>& replicas) {
        replicas_ = replicas;
    }

    inline uint32_t GetTableStat() {
        return table_status_.load(std::memory_order_relaxed);
    }

    inline void SetTableStat(uint32_t table_status) {
        table_status_.store(table_status, std::memory_order_relaxed);
    }

    inline void SetSchema(const std::string& schema) {
        schema_ = schema;
    }

    inline const std::string& GetSchema() {
        return schema_;
    }

    inline void SetExpire(bool is_expire) {
        enable_gc_.store(is_expire, std::memory_order_relaxed);
    }

    uint64_t GetExpireTime();

    bool IsExpire(const LogEntry& entry);

    inline bool GetExpireStatus() {
        return enable_gc_.load(std::memory_order_relaxed);
    }

    inline void SetTimeOffset(int64_t offset) {
        time_offset_.store(offset * 1000, std::memory_order_relaxed); // convert to millisecond
    }

    inline int64_t GetTimeOffset() {
       return  time_offset_.load(std::memory_order_relaxed) / 1000;
    }

    inline std::map<std::string, uint32_t>& GetMapping() {
        return mapping_;
    }

    inline void RecordCntIncr() {
        record_cnt_.fetch_add(1, std::memory_order_relaxed);
    }

    inline void RecordCntIncr(uint32_t cnt) {
        record_cnt_.fetch_add(cnt, std::memory_order_relaxed);
    }

    inline void SetTTLType(const ::rtidb::api::TTLType& type) {
        ttl_type_ = type;
    }

    inline ::rtidb::api::TTLType& GetTTLType() {
        return ttl_type_;
    }

    inline void SetTTL(uint64_t ttl) {
        new_ttl_.store(ttl * 60 * 1000, std::memory_order_relaxed);
    }

    inline uint32_t GetKeyEntryHeight() {
        return key_entry_max_height_;
    }

private:
    std::string const name_;
    uint32_t const id_;
    uint32_t const pid_;
    uint32_t const seg_cnt_;
    uint32_t const idx_cnt_;
    // Segments is readonly
    Segment*** segments_;
    std::atomic<bool> enable_gc_;
    std::atomic<uint64_t> ttl_;
    std::atomic<uint64_t> new_ttl_;
    uint64_t ttl_offset_;
    std::atomic<uint64_t> record_cnt_;
    bool is_leader_;
    std::atomic<int64_t> time_offset_;
    std::vector<std::string> replicas_;
    std::atomic<uint32_t> table_status_;
    std::string schema_;
    std::map<std::string, uint32_t> mapping_;
    bool segment_released_;
    std::atomic<uint64_t> record_byte_size_;
    ::rtidb::api::TTLType ttl_type_;
    ::rtidb::api::CompressType compress_type_;
    uint32_t key_entry_max_height_;
};

}
}


#endif /* !TABLE_H */
