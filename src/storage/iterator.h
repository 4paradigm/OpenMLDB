//
// iterator.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-03-11
//

#pragma once

#include "base/slice.h"

namespace rtidb {
namespace storage {

class TableIterator {
public:
    TableIterator() {}
    virtual ~TableIterator() {}
    TableIterator(const TableIterator&) = delete;
    TableIterator& operator=(const TableIterator&) = delete;
    virtual bool Valid() = 0;
    virtual void Next() = 0;
    virtual rtidb::base::Slice GetValue() const = 0;
    virtual std::string GetPK() const { return std::string();}
    virtual uint64_t GetKey() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() {};
    virtual void Seek(const std::string& pk, uint64_t time) {}
    virtual void Seek(const uint64_t time) {}
    virtual bool Seek(const uint64_t time, ::rtidb::api::GetType type, uint32_t cnt) {} // seek to time use type and count the number of next add to cnt
    virtual bool Seek(const uint64_t time, ::rtidb::api::GetType type) {} // seek to time use type
    virtual void seek(uint64_t time, uint32_t max)
    virtual uint64_t GetCount() const { return 0; }
};

}
}
