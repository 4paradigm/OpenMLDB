/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window.cc
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/
#include "base/window.h"
#include <proto/type.pb.h>
#include <string>
#include "vector"
#include <glog/logging.h>
namespace fesql {
namespace base {
template <class V>
class WindowIteratorImp;
template <class V>
class ColumnIteratorImpl;

template <class V>

class IteratorImpl : public Iterator<V> {
 public:
    explicit IteratorImpl() : list_(), start_(0), end_(0), pos_(0) {}
    explicit IteratorImpl(IteratorImpl<V> &impl)
        : list_(impl.list_),
          start_(impl.start_),
          end_(impl.end_),
          pos_(impl.start_) {}
    explicit IteratorImpl(const std::vector<V> &list)
        : list_(list), start_(0), end_(list.size()), pos_(0) {}
    explicit IteratorImpl(const std::vector<V> &list, int start, int end)
        : list_(list), start_(start), end_(end), pos_(start) {}
    ~IteratorImpl() {}
    bool Valid() const { return pos_ < end_; }

    V Next() { return list_[pos_++]; }

    void reset() { pos_ = start_; }

    IteratorImpl<V> *range(int start, int end) {
        if (start > end || end < start_ || start > end_) {
            return new IteratorImpl(list_, start_, start_);
        }
        start = start < start_ ? start_ : start;
        end = end > end_ ? end_ : end;
        return new IteratorImpl(list_, start, end);
    }

 protected:
    const std::vector<V> list_;
    const int start_;
    const int end_;
    int pos_;
};

class WindowIteratorImpl : public IteratorImpl<Row> {
 public:
    explicit WindowIteratorImpl(const std::vector<Row> &list)
        : IteratorImpl(list) {}
    WindowIteratorImpl(const std::vector<Row> &list, int start, int end)
        : IteratorImpl(list, start, end) {}
};

template <class V, class R>
class WrapIteratorImpl : public IteratorImpl<V> {
 public:
    explicit WrapIteratorImpl(IteratorImpl<R> &root)
        : IteratorImpl<V>(), root_(root) {}
    ~WrapIteratorImpl() {}
    bool Valid() const { return root_.Valid(); }

    V Next() { return GetField(root_.Next()); }
    virtual V GetField(R row) = 0;
    void reset() { root_.reset(); }

    IteratorImpl<V> *range(int start, int end) {
        return root_.range(start, end);
    }

 protected:
    IteratorImpl<R> root_;
};

template <class V>
class ColumnIteratorImpl : public WrapIteratorImpl<V, Row> {
 public:
    ColumnIteratorImpl(IteratorImpl<Row> &impl,
                       int32_t (*get_filed)(int8_t *, int8_t *))
        : WrapIteratorImpl<V, Row>(impl), get_filed_(get_filed) {}

    ColumnIteratorImpl(IteratorImpl<Row> &impl, uint32_t offset)
        : WrapIteratorImpl<V, Row>(impl),
          offset_(offset),
          get_filed_(nullptr) {}
    V GetField(Row row) {
        V value;
        if (get_filed_ != nullptr) {
            get_filed_(row.buf, (int8_t *)(&value));
            return value;
        } else {
            const int8_t *ptr = row.buf + offset_;
            value = *((const V *)ptr);
            return value;
        }
    }

 private:
    uint32_t offset_;
    int32_t (*get_filed_)(int8_t *, int8_t *);
};

}  // namespace base
}  // namespace fesql