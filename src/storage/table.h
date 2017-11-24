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
#include "boost/atomic.hpp"
#include "proto/tablet.pb.h"

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
          const std::vector<std::string>& replicas);

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
    bool Put(const std::string& pk, uint64_t time, DataBlock* row, uint32_t idx);

    class Iterator {
    public:
        Iterator(Segment::Iterator* it);
        ~Iterator();
        bool Valid() const;
        void Next();
        void Seek(const uint64_t& time);
        DataBlock* GetValue() const;
        uint64_t GetKey() const;
        void SeekToFirst();
    private:
        Segment::Iterator* it_;
    };

    // use the first demission
    Table::Iterator* NewIterator(const std::string& pk, Ticket& ticket);

    Table::Iterator* NewIterator(uint32_t index, const std::string& pk, Ticket& ticket);
    // release all memory allocated
    uint64_t Release();

    uint64_t SchedGc();

    uint64_t GetTTL() const {
        return ttl_ / (60 * 1000);
    }

    bool IsExpired(const ::rtidb::api::LogEntry& entry, uint64_t cur_time);

    uint64_t GetRecordIdxCnt();
    bool GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size);

    inline uint64_t GetRecordCnt() const {
        return record_cnt_.load(boost::memory_order_relaxed);
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
        return table_status_.load(boost::memory_order_relaxed);
    }

    inline void SetTableStat(uint32_t table_status) {
        table_status_.store(table_status, boost::memory_order_relaxed);
    }

    inline void SetSchema(const std::string& schema) {
        schema_ = schema;
    }

    inline const std::string& GetSchema() {
        return schema_;
    }

    inline void SetExpire(bool is_expire) {
        enable_gc_.store(is_expire, boost::memory_order_relaxed);
    }

    inline bool GetExpireStatus() {
        return enable_gc_.load(boost::memory_order_relaxed);
    }

    inline void SetTimeOffset(int64_t offset) {
        time_offset_.store(offset * 1000, boost::memory_order_relaxed); // convert to millisecond
    }

    inline int64_t GetTimeOffset() {
       return  time_offset_.load(boost::memory_order_relaxed) / 1000;
    }

    inline std::map<std::string, uint32_t>& GetMapping() {
        return mapping_;
    }

    inline void RecordCntIncr() {
        record_cnt_.fetch_add(1, boost::memory_order_relaxed);
    }

    inline void RecordCntIncr(uint32_t cnt) {
        record_cnt_.fetch_add(cnt, boost::memory_order_relaxed);
    }


private:
    std::string const name_;
    uint32_t const id_;
    uint32_t const pid_;
    uint32_t const seg_cnt_;
    uint32_t const idx_cnt_;
    // Segments is readonly
    Segment*** segments_;
    boost::atomic<uint32_t> ref_;
    boost::atomic<bool> enable_gc_;
    uint64_t const ttl_;
    uint64_t ttl_offset_;
    boost::atomic<uint64_t> record_cnt_;
    bool is_leader_;
    boost::atomic<int64_t> time_offset_;
    std::vector<std::string> replicas_;
    boost::atomic<uint32_t> table_status_;
    std::string schema_;
    std::map<std::string, uint32_t> mapping_;
    bool segment_released_;
};

}
}


#endif /* !TABLE_H */
