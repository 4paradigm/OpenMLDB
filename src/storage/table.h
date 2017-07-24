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

namespace rtidb {
namespace storage {

enum TableGcType {
    kTTL,
    kCountLimit
};

class Table {

public:

    // Create a logic table with table name , table id, table partition id 
    // and segment count
    Table(const std::string& name,
          uint32_t id,
          uint32_t pid,
          uint32_t seg_cnt,
          uint32_t ttl,
          bool is_leader,
          const std::vector<std::string>& replicas,
          bool wal = true);

    Table(const std::string& name,
          uint32_t id,
          uint32_t pid,
          uint32_t seg_cnt,
          uint32_t ttl,
          bool wal = true);

    void Init();

    void SetGcSafeOffset(uint64_t offset);

    // Put a record
    void Put(const std::string& pk,
             uint64_t time,
             const char* data,
             uint32_t size);

    void BatchGet(const std::vector<std::string>& keys,
                  std::map<uint32_t, DataBlock*>& pairs,
                  Ticket& ticket);

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

    Table::Iterator* NewIterator(const std::string& pk, Ticket& ticket);

    void Ref();

    void UnRef();

    // release all memory allocated
    uint64_t Release();

    uint64_t SchedGc();

    uint32_t GetTTL() const {
        return ttl_;
    }

    inline bool GetWal() {
        return wal_;
    }

    inline void SetTerm(uint64_t term) {
        term_ = term;
    }

    inline uint64_t GetTerm() {
        return term_;
    }

    inline uint64_t GetDataCnt() const {
        uint64_t data_cnt = 0;
        for (uint32_t i = 0; i < seg_cnt_; i++) {
            data_cnt += segments_[i]->GetDataCnt();
        }
        return data_cnt;
    }

    inline void GetDataCnt(uint64_t** stat, uint32_t* size) const {
        if (stat == NULL) {
            return;
        }
        uint64_t* data_array = new uint64_t[seg_cnt_];
        for (uint32_t i = 0; i < seg_cnt_; i++) {
            data_array[i] = segments_[i]->GetDataCnt();
        }
        *stat = data_array;
        *size = seg_cnt_;
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

    inline uint32_t GetPid() const {
        return pid_;
    }

    inline bool IsLeader() const {
        return is_leader_;
    }

    inline const std::vector<std::string>& GetReplicas() const {
        return replicas_;
    }

private:

    ~Table(){}

private:
    std::string const name_;
    uint32_t const id_;
    uint32_t const pid_;
    uint32_t const seg_cnt_;
    // Segments is readonly
    Segment** segments_;
    boost::atomic<uint32_t> ref_;
    bool enable_gc_;
    uint32_t const ttl_;
    uint64_t ttl_offset_;
    boost::atomic<uint64_t> data_cnt_;
    bool const is_leader_;
    std::vector<std::string> const replicas_;
    bool wal_;
    uint64_t term_;
};

}
}


#endif /* !TABLE_H */
