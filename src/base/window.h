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
namespace fesql {
namespace base {

struct Row {
    int8_t *buf;
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
    IteratorImpl();
    explicit IteratorImpl(const IteratorImpl<V> &impl);

    explicit IteratorImpl(const std::vector<V> &list);

    explicit IteratorImpl(const std::vector<V> &list, int start, int end);
    ~IteratorImpl();
    bool Valid() const;

    V Next();

    void reset();

    IteratorImpl<V> *range(int start, int end);

 protected:
    const std::vector<V> list_;
    const int start_;
    const int end_;
    int pos_;
};

class WindowIteratorImpl : public IteratorImpl<Row> {
 public:
    explicit WindowIteratorImpl(const std::vector<Row> &list);
    WindowIteratorImpl(const std::vector<Row> &list, int start, int end);
};

template <class V, class R>
class WrapIteratorImpl : public IteratorImpl<V> {
 public:
    explicit WrapIteratorImpl(const IteratorImpl<R> &root);

    ~WrapIteratorImpl();
    bool Valid() const;

    V Next();
    virtual V GetField(R row) = 0;
    void reset();

    IteratorImpl<V> *range(int start, int end);

 protected:
    IteratorImpl<R> root_;
};

template <class V>
class ColumnIteratorImpl : public WrapIteratorImpl<V, Row> {
 public:
    ColumnIteratorImpl(const IteratorImpl<Row> &impl,
                       int32_t (*get_filed)(int8_t *, int8_t *));

    ColumnIteratorImpl(const IteratorImpl<Row> &impl, uint32_t offset);

    V GetField(Row row);

 private:
    uint32_t offset_;
    int32_t (*get_filed_)(int8_t *, int8_t *);
};


template <class V>
IteratorImpl<V>::IteratorImpl() : list_(), start_(0), end_(0), pos_(0) {}

template <class V>
IteratorImpl<V>::IteratorImpl(const IteratorImpl<V> &impl)
    : list_(impl.list_),
      start_(impl.start_),
      end_(impl.end_),
      pos_(impl.start_) {}

template <class V>
IteratorImpl<V>::IteratorImpl(const std::vector<V> &list)
    : list_(list), start_(0), end_(list.size()), pos_(0) {}
template <class V>
IteratorImpl<V>::IteratorImpl(const std::vector<V> &list, int start, int end)
    : list_(list), start_(start), end_(end), pos_(start) {}
template <class V>
IteratorImpl<V>::~IteratorImpl() {}
template <class V>
bool IteratorImpl<V>::Valid() const {
    return pos_ < end_;
}
template <class V>
V IteratorImpl<V>::Next() {
    return list_[pos_++];
}
template <class V>
void IteratorImpl<V>::reset() {
    pos_ = start_;
}
template <class V>
IteratorImpl<V> *IteratorImpl<V>::range(int start, int end) {
    if (start > end || end < start_ || start > end_) {
        return new IteratorImpl(list_, start_, start_);
    }
    start = start < start_ ? start_ : start;
    end = end > end_ ? end_ : end;
    return new IteratorImpl(list_, start, end);
}



template <class V, class R>
WrapIteratorImpl<V, R>::WrapIteratorImpl(const IteratorImpl<R> &root)
    : IteratorImpl<V>(), root_(root) {}
template <class V, class R>
WrapIteratorImpl<V, R>::~WrapIteratorImpl() {}
template <class V, class R>
bool WrapIteratorImpl<V, R>::Valid() const {
    return root_.Valid();
}
template <class V, class R>
V WrapIteratorImpl<V, R>::Next() {
    return GetField(root_.Next());
}
template <class V, class R>
void WrapIteratorImpl<V, R>::reset() {
    root_.reset();
}
template <class V, class R>
IteratorImpl<V> *WrapIteratorImpl<V, R>::range(int start, int end) {
    return root_.range(start, end);
}


WindowIteratorImpl::WindowIteratorImpl(const std::vector<Row> &list)
    : IteratorImpl<Row>(list) {}
WindowIteratorImpl::WindowIteratorImpl(const std::vector<Row> &list, int start,
                                       int end)
    : IteratorImpl<Row>(list, start, end) {}


template <class V>
ColumnIteratorImpl<V>::ColumnIteratorImpl(const IteratorImpl<Row> &impl,
                                          int32_t (*get_filed)(int8_t *,
                                                               int8_t *))
    : WrapIteratorImpl<V, Row>(impl), get_filed_(get_filed) {}
template <class V>
ColumnIteratorImpl<V>::ColumnIteratorImpl(const IteratorImpl<Row> &impl,
                                          uint32_t offset)
    : WrapIteratorImpl<V, Row>(impl), offset_(offset), get_filed_(nullptr) {}
template <class V>
V ColumnIteratorImpl<V>::GetField(const Row row) {
    V value;
    if (get_filed_ != nullptr) {
        get_filed_(row.buf, reinterpret_cast<int8_t *>(&value));
        return value;
    } else {
        const int8_t *ptr = row.buf + offset_;
        value = *((const V *)ptr);
        return value;
    }
}

}  // namespace base
}  // namespace fesql

#endif  // SRC_BASE_WINDOW_H_
