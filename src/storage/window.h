/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window.h
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BASE_WINDOW_H_
#define SRC_BASE_WINDOW_H_

#include <cstdint>
#include <vector>
#include "storage/type_ir_builder.h"
namespace fesql {
namespace storage {

struct Row {
    int8_t *buf;
    uint32_t size;
};

template <class V>
class IteratorV {
 public:
    IteratorV() {}
    virtual ~IteratorV() {}
    virtual bool Valid() const = 0;
    virtual V Next() = 0;
    virtual void reset() = 0;
};

template <class V>
class IteratorImpl : public IteratorV<V> {
 public:
    IteratorImpl() : list_(), start_(0), end_(0), pos_(0) {}

    explicit IteratorImpl(const IteratorImpl<V> &impl)
        : list_(impl.list_),
          start_(impl.start_),
          end_(impl.end_),
          pos_(impl.start_) {}

    explicit IteratorImpl(const std::vector<V> &list)
        : list_(list), start_(0), end_(list.size()), pos_(0) {}

    IteratorImpl(const std::vector<V> &list, int start, int end)
        : list_(list), start_(start), end_(end), pos_(start) {}
    ~IteratorImpl(){};
    bool Valid() const {
        return pos_ < end_;
    }

    V Next() {
        return list_[pos_++];
    }

    void reset() {
        pos_ = start_;
    }

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
        : IteratorImpl<Row>(list) {}
    WindowIteratorImpl(const std::vector<Row> &list, int start,
                                           int end)
        : IteratorImpl<Row>(list, start, end) {}
};

template <class V, class R>
class WrapIteratorImpl : public IteratorImpl<V> {
 public:
    explicit WrapIteratorImpl(const IteratorImpl<R> &root)
        : IteratorImpl<V>(), root_(root) {}

    ~WrapIteratorImpl() {}
    bool Valid() const {
        return root_.Valid();
    }

    V Next() {
        return GetField(root_.Next());
    }
    virtual V GetField(R row) = 0;
    void reset() {
        root_.reset();
    }

    IteratorImpl<V> *range(int start, int end) {
        return root_.range(start, end);
    }

 protected:
    IteratorImpl<R> root_;
};

template <class V>
class ColumnIteratorImpl : public WrapIteratorImpl<V, Row> {
 public:
    ColumnIteratorImpl(const IteratorImpl<Row> &impl,
                       uint32_t offset)
        : WrapIteratorImpl<V, Row>(impl), offset_(offset) {}

    V GetField(const Row row) {
        V value;
        const int8_t *ptr = row.buf + offset_;
        value = *((const V *)ptr);
        return value;
    }

 private:
    uint32_t offset_;
};
class ColumnStringIteratorImpl
    : public ColumnIteratorImpl<fesql::storage::StringRef> {
 public:
    ColumnStringIteratorImpl(const IteratorImpl<Row> &impl,
                             int32_t str_field_offset,
                             int32_t next_str_field_offset,
                             int32_t str_start_offset);

    fesql::storage::StringRef GetField(Row row) {
        int32_t addr_space = fesql::storage::v1::GetAddrSpace(row.size);
        DLOG(INFO) << "row size: " << row.size << " addr_space: " << addr_space;
        fesql::storage::StringRef value;
        fesql::storage::v1::GetStrField(row.buf, str_field_offset_,
                                        next_str_field_offset_, str_start_offset_,
                                        addr_space, reinterpret_cast<int8_t **>(&(value.data)), &(value.size));
        DLOG(INFO) << "value.size " << value.size;
        return value;
    }

 private:
    uint32_t str_field_offset_;
    uint32_t next_str_field_offset_;
    uint32_t str_start_offset_;
};

}  // namespace storage
}  // namespace fesql

#endif  // SRC_BASE_WINDOW_H_
