#pragma once

#include <string>

namespace openmldb {
namespace base {

template <class K, class V>
class BaseIterator {
 public:
    BaseIterator() {}  // NOLINT
    virtual ~BaseIterator() {}

    virtual bool Valid() const = 0;

    virtual void Next() = 0;

    virtual const K& GetKey() const = 0;

    virtual V& GetValue() = 0;

    virtual void Seek(const K& k) = 0;

    virtual void SeekToFirst() = 0;

    virtual void SeekToLast() = 0;

    virtual uint32_t GetSize() = 0;
};

}  // namespace base
}  // namespace openmldb
