/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window.h
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEC_LIST_ITERATOR_CODEC_H_
#define SRC_CODEC_LIST_ITERATOR_CODEC_H_
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/fe_slice.h"
#include "base/iterator.h"
#include "codec/row.h"
#include "codec/type_codec.h"
#include "glog/logging.h"
namespace fesql {
namespace codec {

using fesql::base::ConstIterator;
template <class V>
class ArrayListIterator;

template <class V>
class ColumnImpl;

template <class V>
class ColumnIterator;

typedef ConstIterator<uint64_t, Row> RowIterator;
class WindowIterator {
 public:
    WindowIterator() {}
    virtual ~WindowIterator() {}
    virtual void Seek(const std::string &key) = 0;
    virtual void SeekToFirst() = 0;
    virtual void Next() = 0;
    virtual bool Valid() = 0;
    virtual std::unique_ptr<RowIterator> GetValue() = 0;
    virtual const Row GetKey() = 0;
};

template <class V>
class ListV {
 public:
    ListV() {}
    virtual ~ListV() {}
    // TODO(chenjing): at 数组越界处理
    virtual std::unique_ptr<ConstIterator<uint64_t, V>> GetIterator() const = 0;
    virtual ConstIterator<uint64_t, V> *GetIterator(int8_t *addr) const = 0;
    virtual const uint64_t GetCount() = 0;
    virtual V At(uint64_t pos) = 0;
};

template <class V, class R>
class WrapListImpl : public ListV<V> {
 public:
    WrapListImpl() : ListV<V>() {}
    ~WrapListImpl() {}
    virtual const V GetField(R row) const = 0;
};

template <class V>
class ColumnImpl : public WrapListImpl<V, Row> {
 public:
    ColumnImpl(ListV<Row> *impl, int32_t row_idx, uint32_t offset)
        : WrapListImpl<V, Row>(),
          root_(impl),
          row_idx_(row_idx),
          offset_(offset) {}

    ~ColumnImpl() {}
    const V GetField(Row row) const override {
        V value;
        const int8_t *ptr = row.buf(row_idx_) + offset_;
        value = *((const V *)ptr);
        return value;
    }
    std::unique_ptr<ConstIterator<uint64_t, V>> GetIterator() const override {
        auto iter = std::unique_ptr<ConstIterator<uint64_t, V>>(
            new ColumnIterator<V>(root_, this));
        return std::move(iter);
    }
    ConstIterator<uint64_t, V> *GetIterator(int8_t *addr) const override {
        if (nullptr == addr) {
            return new ColumnIterator<V>(root_, this);
        } else {
            return new (addr) ColumnIterator<V>(root_, this);
        }
    }
    const uint64_t GetCount() override { return root_->GetCount(); }
    V At(uint64_t pos) override { return GetField(root_->At(pos)); }

 protected:
    ListV<Row> *root_;
    const uint32_t row_idx_;
    const uint32_t offset_;
};

class StringColumnImpl : public ColumnImpl<StringRef> {
 public:
    StringColumnImpl(ListV<Row> *impl, int32_t row_idx,
                     int32_t str_field_offset, int32_t next_str_field_offset,
                     int32_t str_start_offset)
        : ColumnImpl<StringRef>(impl, row_idx, 0u),
          str_field_offset_(str_field_offset),
          next_str_field_offset_(next_str_field_offset),
          str_start_offset_(str_start_offset) {}

    ~StringColumnImpl() {}
    const StringRef GetField(Row row) const override {
        int32_t addr_space = v1::GetAddrSpace(row.size(row_idx_));
        StringRef value;
        v1::GetStrField(row.buf(row_idx_), str_field_offset_,
                        next_str_field_offset_, str_start_offset_, addr_space,
                        reinterpret_cast<int8_t **>(&(value.data)),
                        &(value.size));
        return value;
    }

 private:
    uint32_t str_field_offset_;
    uint32_t next_str_field_offset_;
    uint32_t str_start_offset_;
};

template <class V>
class ArrayListV : public ListV<V> {
 public:
    ArrayListV() : start_(0), end_(0), buffer_(nullptr) {}
    explicit ArrayListV(std::vector<V> *buffer)
        : start_(0), end_(buffer->size()), buffer_(buffer) {}

    ArrayListV(std::vector<V> *buffer, uint32_t start, uint32_t end)
        : start_(start), end_(end), buffer_(buffer) {}

    ~ArrayListV() {}
    // TODO(chenjing): at 数组越界处理

    std::unique_ptr<ConstIterator<uint64_t, V>> GetIterator() const override {
        return std::unique_ptr<ArrayListIterator<V>>(
            new ArrayListIterator<V>(buffer_, start_, end_));
    }
    ConstIterator<uint64_t, V> *GetIterator(int8_t *addr) const override {
        if (nullptr == addr) {
            return new ArrayListIterator<V>(buffer_, start_, end_);
        } else {
            return new (addr) ArrayListIterator<V>(buffer_, start_, end_);
        }
    }
    virtual const uint64_t GetCount() { return end_ - start_; }
    virtual V At(uint64_t pos) { return buffer_->at(start_ + pos); }

 protected:
    uint64_t start_;
    uint64_t end_;
    std::vector<V> *buffer_;
};

template <class V>
class ArrayListIterator : public ConstIterator<uint64_t, V> {
 public:
    explicit ArrayListIterator(const std::vector<V> *buffer,
                               const uint64_t start, const uint64_t end)
        : buffer_(buffer),
          iter_start_(buffer->cbegin() + start),
          iter_end_(buffer->cbegin() + end),
          iter_(iter_start_),
          key_(0) {}

    explicit ArrayListIterator(const ArrayListIterator<V> &impl)
        : buffer_(impl.buffer_),
          iter_start_(impl.iter_start_),
          iter_end_(impl.iter_end_),
          iter_(impl.iter_start_),
          key_(0) {}
    explicit ArrayListIterator(const ArrayListIterator<V> &impl, uint64_t start,
                               uint64_t end)
        : buffer_(impl.buffer_),
          iter_start_(impl.iter_start_ + start),
          iter_end_(impl.iter_start_ + end),
          iter_(iter_start_),
          key_(0) {}

    ~ArrayListIterator() {}
    void Seek(const uint64_t &key) override {
        iter_ =
            (iter_start_ + key) >= iter_end_ ? iter_end_ : iter_start_ + key;
        key_ = iter_end_ - iter_start_;
    }

    bool Valid() const override { return iter_end_ != iter_; }

    void Next() override { iter_++; }

    const V &GetValue() override { return *iter_; }

    const uint64_t &GetKey() const override { return key_; }

    void SeekToFirst() { iter_ = iter_start_; }

    bool IsSeekable() const override { return true; }
    ArrayListIterator<V> *range(int start, int end) {
        if (start > end || end < iter_start_ || start > iter_end_) {
            return new ArrayListIterator(buffer_, iter_start_, iter_start_);
        }
        start = start < iter_start_ ? iter_start_ : start;
        end = end > iter_end_ ? iter_end_ : end;
        return new ArrayListIterator(buffer_, start, end);
    }

 protected:
    const std::vector<V> *buffer_;
    const typename std::vector<V>::const_iterator iter_start_;
    const typename std::vector<V>::const_iterator iter_end_;
    typename std::vector<V>::const_iterator iter_;
    uint64_t key_;
};

template <class V>
class ColumnIterator : public ConstIterator<uint64_t, V> {
 public:
    ColumnIterator(ListV<Row> *list, const ColumnImpl<V> *column_impl)
        : ConstIterator<uint64_t, V>(), column_impl_(column_impl) {
        row_iter_ = list->GetIterator();
        if (!row_iter_) {
            row_iter_->SeekToFirst();
        }
    }
    ~ColumnIterator() {}
    void Seek(const uint64_t &key) override { row_iter_->Seek(key); }
    void SeekToFirst() override { row_iter_->SeekToFirst(); }
    bool Valid() const override { return row_iter_->Valid(); }
    void Next() override { row_iter_->Next(); }
    const V &GetValue() override {
        value_ = column_impl_->GetField(row_iter_->GetValue());
        return value_;
    }
    const uint64_t &GetKey() const override { return row_iter_->GetKey(); }
    bool IsSeekable() const override { return row_iter_->IsSeekable(); }

 private:
    const ColumnImpl<V> *column_impl_;
    std::unique_ptr<ConstIterator<uint64_t, Row>> row_iter_;
    V value_;
};

}  // namespace codec
}  // namespace fesql

#endif  // SRC_CODEC_LIST_ITERATOR_CODEC_H_
