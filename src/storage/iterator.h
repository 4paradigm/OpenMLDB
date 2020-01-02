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
    virtual void Seek(uint64_t time) {}
    virtual uint64_t GetCount() const { return 0; }
};

}
}
