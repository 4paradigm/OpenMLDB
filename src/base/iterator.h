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

template <class K, class V, class Ref>
class AbstractIterator {
 public:
    AbstractIterator() {}
    AbstractIterator(const AbstractIterator&) = delete;
    AbstractIterator& operator=(const AbstractIterator&) = delete;
    virtual ~AbstractIterator() {}
    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual const K& GetKey() const = 0;
    virtual Ref GetValue() = 0;
    virtual void Seek(const K& k) = 0;
    virtual void SeekToFirst() = 0;
    virtual bool IsSeekable() const = 0;
};

template <class K, class V>
class Iterator : public AbstractIterator<K, V, V&> {};

template <class K, class V>
class ConstIterator : public fesql::base::AbstractIterator<K, V, const V&> {};
}  // namespace base
}  // namespace fesql
