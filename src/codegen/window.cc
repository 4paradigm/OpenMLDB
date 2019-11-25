/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window.cc
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/
#include "codegen/window.h"
#include "vector"
namespace fesql {
namespace codegen {

class IteratorImpl;
class WindowIteratorImpl;
class ColumnIteratorImpl;
class IteratorImpl<V> : public Iterator<V> {
 public:
    explicit IteratorImpl(const std::vector<V> &list)
        : list_(list), pos_(0), start_(0) {
        end_ = list.size();
    }
    explicit IteratorImpl(const std::vector<V> &list, int start, int end)
        : list_(list), pos_(start), start_(start), end_(end) {}
    ~IteratorImpl() {}
    bool Valid() const { return pos_ < end_; }

    V Next() { return list[pos_++]; }

    void reset() { pos_ = start_; }

    IteratorImpl<V> range(int start, int end) {
        if (start > end || end < start_ || start > end_) {
            return new EmptyIteratorImpl();
        }
        start = start < start_ ? start_ : start;
        end = end > end_ ? end_ : end;
        return new IteratorImpl(list_, start, end);
    }

 protected:
    const std::vector<V> list_;
    int pos_;
    const int start_;
    const int end_;
};

class EmptyIteratorImpl<V> : public IteratorImpl<V> {
 public:
    EmptyIteratorImpl() {}
    ~EmptyIteratorImpl() {}
    bool Valid() const { return false; }
    V Next() {}
    void reset() {}
    IteratorImpl<V> range(int start, int end) {
        return new EmptyIteratorImpl();
    }
};

struct Row {
    char *buf;
    int len;
};

class WindowIteratorImpl : public IteratorImpl<Row> {
 public:
    ColumnIteratorImpl &col(const std::string &col) {
        return new ColumnIteratorImpl(list_, start, end, col);
    }

};

class ColumnIteratorImpl : public IteratorImpl<V> {
 public:
    ColumnIteratorImpl(std::vector<Row> &list, const int start, const int end,
                       const std::string &col)
        : IteratorImpl(list, start, end), pos_(0), col_(col) {}
    bool Valid() const { return w_iter_.Valid(); }
    V Next() {
        Row row = list_[pos_++];
        V value;
        decode(row.buf, (int8_t*)(&value));
    }
 private:
    V GetRowFiled() {
    }
 private:
    const std::string col_;
    int32_t (*decode)(int8_t*, int8_t*);
};

}  // namespace codegen
}