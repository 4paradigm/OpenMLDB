//
// table.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31 
// 


#ifndef RTIDB_STORAGE_TABLE_H
#define RTIDB_STORAGE_TABLE_H

#include "storage/segment.h"
#include "boost/atomic.hpp"

namespace rtidb {
namespace storage {

class Table {

public:
    // Create a logic table with table name , table id, table partition id 
    // and segment count
    Table(const std::string& name,
          uint32_t id,
          uint32_t pid,
          uint32_t seg_cnt);

    ~Table();

    void Init();

    // Put 
    void Put(const std::string& pk,
             const uint64_t& time,
             const char* data,
             uint32_t size);

    class Iterator {
    public:
        Iterator(Segment::Iterator* it);
        ~Iterator();
        
        bool Valid() const;
        void Next();
        void Seek(const uint64_t& time);
        DataBlock* GetValue() const;
        uint64_t GetKey() const;
    private:
        Segment::Iterator* it_;
    };

    Table::Iterator* NewIterator(const std::string& pk);

private:
    std::string const name_;
    uint32_t const id_;
    uint32_t const pid_;
    uint32_t const seg_cnt_;
    // Segments is readonly
    Segment** segments_;
    boost::atomic<uint32_t> ref_;
};

}
}


#endif /* !TABLE_H */
