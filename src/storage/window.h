/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window.h
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_STORAGE_WINDOW_H_
#define SRC_STORAGE_WINDOW_H_
#include <vm/mem_catalog.h>
#include <cstdint>
#include <iostream>
#include <utility>
#include <vector>
#include "base/slice.h"
#include "storage/type_native_fn.h"
#include "vm/catalog.h"
namespace fesql {
namespace storage {
using base::Slice;
using vm::ListV;

template <class V>
class ArrayListIterator;

template <class V>
class ColumnImpl;

template <class V>
class ColumnIterator;

template <class V, class R>
class WrapListImpl : public ListV<V> {
 public:
    explicit WrapListImpl() : ListV<V>() {}
    ~WrapListImpl() {}
    virtual const V GetField(R row) const = 0;
};

template <class V>
class ColumnImpl : public WrapListImpl<V, Slice> {
 public:
    ColumnImpl(vm::ListV<Slice> *impl, uint32_t offset)
        : WrapListImpl<V, Slice>(), root_(impl), offset_(offset) {}

    ~ColumnImpl() {}
    const V GetField(Slice row) const override {
        V value;
        const int8_t *ptr = row.buf() + offset_;
        value = *((const V *)ptr);
        return value;
    }
    std::unique_ptr<vm::IteratorV<uint64_t, V>> GetIterator() const override {
        auto iter = std::unique_ptr<vm::IteratorV<uint64_t, V>>(
            new ColumnIterator<V>(root_->GetIterator(nullptr), this));
        return std::move(iter);
    }
    vm::IteratorV<uint64_t, V> *GetIterator(int8_t *addr) const override {
        if (nullptr == addr) {
            return new ColumnIterator<V>(root_->GetIterator(nullptr), this);
        } else {
            return new (addr)
                ColumnIterator<V>(root_->GetIterator(nullptr), this);
        }
    }
    const uint64_t GetCount() override { return root_->GetCount(); }
    V At(uint64_t pos) override { return GetField(root_->At(pos)); }

 private:
    vm::ListV<Slice> *root_;
    const uint32_t offset_;
};

class StringColumnImpl : public ColumnImpl<fesql::storage::StringRef> {
 public:
    StringColumnImpl(vm::ListV<Slice> *impl, int32_t str_field_offset,
                     int32_t next_str_field_offset, int32_t str_start_offset)
        : ColumnImpl<::fesql::storage::StringRef>(impl, 0u),
          str_field_offset_(str_field_offset),
          next_str_field_offset_(next_str_field_offset),
          str_start_offset_(str_start_offset) {}

    ~StringColumnImpl() {}
    const StringRef GetField(Slice row) const override {
        int32_t addr_space = fesql::storage::v1::GetAddrSpace(row.size());
        fesql::storage::StringRef value;
        fesql::storage::v1::GetStrField(
            row.buf(), str_field_offset_, next_str_field_offset_,
            str_start_offset_, addr_space,
            reinterpret_cast<int8_t **>(&(value.data)), &(value.size));
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

    std::unique_ptr<vm::IteratorV<uint64_t, V>> GetIterator() const override {
        return std::unique_ptr<ArrayListIterator<V>>(
            new ArrayListIterator<V>(buffer_, start_, end_));
    }
    vm::IteratorV<uint64_t, V> *GetIterator(int8_t *addr) const override {
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
class ArrayListIterator : public vm::IteratorV<uint64_t, V> {
 public:
    explicit ArrayListIterator(const std::vector<V> *buffer,
                               const uint64_t start, const uint64_t end)
        : buffer_(buffer),
          iter_start_(buffer->cbegin() + start),
          iter_end_(buffer->cbegin() + end),
          iter_(iter_start_) {}

    explicit ArrayListIterator(const ArrayListIterator<V> &impl)
        : buffer_(impl.buffer_),
          iter_start_(impl.iter_start_),
          iter_end_(impl.iter_end_),
          iter_(impl.iter_start_) {}

    explicit ArrayListIterator(const ArrayListIterator<V> &impl, uint64_t start,
                               uint64_t end)
        : buffer_(impl.buffer_),
          iter_start_(impl.iter_start_ + start),
          iter_end_(impl.iter_start_ + end),
          iter_(iter_start_) {}

    ~ArrayListIterator() {}
    void Seek(uint64_t key) override {
        iter_ =
            (iter_start_ + key) >= iter_end_ ? iter_end_ : iter_start_ + key;
    }

    bool Valid() override { return iter_end_ != iter_; }

    void Next() override { iter_++; }

    const V GetValue() override { return *iter_; }

    const uint64_t GetKey() override { return iter_ - iter_start_; }

    void SeekToFirst() { iter_ = iter_start_; }

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
};

template <class V>
class ColumnIterator : public vm::IteratorV<uint64_t, V> {
 public:
    ColumnIterator(vm::IteratorV<uint64_t, Slice> *row_iter,
                   const ColumnImpl<V> *column_impl)
        : vm::IteratorV<uint64_t, V>(), column_impl_(column_impl) {
        row_iter_ = row_iter;
    }
    ~ColumnIterator() {}
    void Seek(uint64_t key) override { row_iter_->Seek(key); }
    void SeekToFirst() override { row_iter_->SeekToFirst(); }
    bool Valid() override { return row_iter_->Valid(); }
    void Next() override { row_iter_->Next(); }
    const V GetValue() override {
        return column_impl_->GetField(row_iter_->GetValue());
    }
    const uint64_t GetKey() override { return row_iter_->GetKey(); }

 private:
    vm::IteratorV<uint64_t, Slice> *row_iter_;
    const ColumnImpl<V> *column_impl_;
};

typedef ArrayListV<Slice> WindowImpl;
}  // namespace storage
}  // namespace fesql

#endif  // SRC_STORAGE_WINDOW_H_
