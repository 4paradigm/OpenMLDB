// iterator.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-10-31
//

#pragma once
#include <stdint.h>

namespace fesql {
namespace base {

struct DefaultComparator {
    int operator()(const uint64_t a, const uint64_t b) const {
        if (a > b) {
            return -1;
        } else if (a == b) {
            return 0;
        }
        return 1;
    }
};

template <class K, class V>
class Iterator {
 public:
    Iterator() {}
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;
    virtual ~Iterator() {}
    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual const K& GetKey() const = 0;
    virtual V& GetValue() = 0;
    virtual void Seek(const K& k) = 0;
    virtual void SeekToFirst() = 0;
    virtual bool IsSeekable() const = 0;
};

}  // namespace base
}  // namespace fesql
