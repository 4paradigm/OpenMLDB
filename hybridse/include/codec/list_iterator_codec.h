/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_INCLUDE_CODEC_LIST_ITERATOR_CODEC_H_
#define HYBRIDSE_INCLUDE_CODEC_LIST_ITERATOR_CODEC_H_

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "base/iterator.h"
#include "base/string_ref.h"
#include "codec/row.h"
#include "codec/row_list.h"
#include "codec/type_codec.h"

namespace hybridse {
namespace codec {

using openmldb::base::StringRef;

template <class V>
class ArrayListIterator;

template <class V>
class ColumnImpl;

template <class V>
class ColumnIterator;

template <class V>
class NonNullColumnIterator;

template <class V>
class NonNullColumnList;

// table column list. remember column values might be NULL
template <class V>
class ColumnImpl : public ListV<V> {
 public:
    ColumnImpl(ListV<Row> *impl, int32_t row_idx, uint32_t col_idx, uint32_t offset)
        : ListV<V>(), root_(impl), row_idx_(row_idx), col_idx_(col_idx), offset_(offset) {}

    ~ColumnImpl() override {}

    virtual const V GetFieldUnsafe(const Row &row) const {
        V value;
        const int8_t *ptr = row.buf(row_idx_) + offset_;
        value = *((const V *)ptr);
        return value;
    }

    virtual void GetField(const Row &row, V *res, bool *is_null) const {
        const int8_t *buf = row.buf(row_idx_);
        if (buf == nullptr || v1::IsNullAt(buf, col_idx_)) {
            *is_null = true;
        } else {
            *is_null = false;
            const int8_t *ptr = buf + offset_;
            *res = *((const V *)ptr);
        }
    }

    virtual const bool IsNull(const Row &row) const {
        const int8_t *buf = row.buf(row_idx_);
        return buf == nullptr || v1::IsNullAt(buf, col_idx_);
    }

    // TODO(xxx): iterator of nullable V
    ConstIterator<uint64_t, V> *GetRawIterator() override {
        return new ColumnIterator<V>(root_->GetIterator(), this);
    }

    NonNullColumnList<V>* GetAsNonNullColumnList() {
        return new NonNullColumnList<V>(this);
    }

    ListV<Row>* root() const { return root_; }

 protected:
    ListV<Row> *root_;
    const uint32_t row_idx_;
    const uint32_t col_idx_;
    const uint32_t offset_;
};

class StringColumnImpl : public ColumnImpl<StringRef> {
 public:
    StringColumnImpl(ListV<Row> *impl, int32_t row_idx, uint32_t col_idx, int32_t str_field_offset,
                     int32_t next_str_field_offset, int32_t str_start_offset)
        : ColumnImpl<StringRef>(impl, row_idx, col_idx, 0u),
          str_field_offset_(str_field_offset),
          next_str_field_offset_(next_str_field_offset),
          str_start_offset_(str_start_offset) {}

    ~StringColumnImpl() {}
    const StringRef GetFieldUnsafe(const Row &row) const override {
        int32_t addr_space = v1::GetAddrSpace(row.size(row_idx_));
        StringRef value;
        const char *buffer;
        v1::GetStrFieldUnsafe(row.buf(row_idx_), col_idx_, str_field_offset_,
                              next_str_field_offset_, str_start_offset_,
                              addr_space, &buffer, &(value.size_));
        value.data_ = buffer;
        return value;
    }

    void GetField(const Row &row, StringRef *res,
                  bool *is_null) const override {
        const int8_t *buf = row.buf(row_idx_);
        if (buf == nullptr || v1::IsNullAt(buf, col_idx_)) {
            *is_null = true;
        } else {
            *is_null = false;
            int32_t addr_space = v1::GetAddrSpace(row.size(row_idx_));
            StringRef value;
            const char *buffer;
            v1::GetStrFieldUnsafe(buf, col_idx_, str_field_offset_,
                                  next_str_field_offset_, str_start_offset_,
                                  addr_space, &buffer, &(value.size_));
            value.data_ = buffer;
            *res = value;
        }
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

    std::unique_ptr<ConstIterator<uint64_t, V>> GetIterator() override {
        return std::unique_ptr<ArrayListIterator<V>>(
            new ArrayListIterator<V>(buffer_, start_, end_));
    }
    ConstIterator<uint64_t, V> *GetRawIterator() override {
        return new ArrayListIterator<V>(buffer_, start_, end_);
    }
    virtual const uint64_t GetCount() { return end_ - start_; }
    virtual V At(uint64_t pos) const { return buffer_->at(start_ + pos); }

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

    ArrayListIterator(const ArrayListIterator<V> &impl)
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

    void Next() override { ++iter_; }

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

class BoolArrayListIterator : public ConstIterator<uint64_t, bool> {
 public:
    explicit BoolArrayListIterator(const std::vector<int> *buffer,
                                   const uint64_t start, const uint64_t end)
        : buffer_(buffer),
          iter_start_(buffer->cbegin() + start),
          iter_end_(buffer->cbegin() + end),
          iter_(iter_start_),
          key_(0) {
        if (Valid()) {
            tmp_ = *iter_;
        }
    }

    BoolArrayListIterator(const BoolArrayListIterator &impl)
        : buffer_(impl.buffer_),
          iter_start_(impl.iter_start_),
          iter_end_(impl.iter_end_),
          iter_(impl.iter_start_),
          key_(0) {
        if (Valid()) {
            tmp_ = *iter_;
        }
    }
    explicit BoolArrayListIterator(const BoolArrayListIterator &impl,
                                   uint64_t start, uint64_t end)
        : buffer_(impl.buffer_),
          iter_start_(impl.iter_start_ + start),
          iter_end_(impl.iter_start_ + end),
          iter_(iter_start_),
          key_(0) {
        if (Valid()) {
            tmp_ = *iter_;
        }
    }

    ~BoolArrayListIterator() {}
    void Seek(const uint64_t &key) override {
        iter_ =
            (iter_start_ + key) >= iter_end_ ? iter_end_ : iter_start_ + key;
        key_ = iter_end_ - iter_start_;
    }

    bool Valid() const override { return iter_end_ != iter_; }

    void Next() override {
        ++iter_;
        if (Valid()) {
            tmp_ = *iter_;
        }
    }

    const bool &GetValue() override { return tmp_; }

    const uint64_t &GetKey() const override { return key_; }

    void SeekToFirst() {
        iter_ = iter_start_;
        if (Valid()) {
            tmp_ = *iter_;
        }
    }

    bool IsSeekable() const override { return true; }

 protected:
    const std::vector<int> *buffer_;
    const typename std::vector<int>::const_iterator iter_start_;
    const typename std::vector<int>::const_iterator iter_end_;
    typename std::vector<int>::const_iterator iter_;
    uint64_t key_;
    bool tmp_;
};

class BoolArrayListV : public ListV<bool> {
 public:
    BoolArrayListV() : start_(0), end_(0), buffer_(nullptr) {}
    explicit BoolArrayListV(std::vector<int> *buffer)
        : start_(0), end_(buffer->size()), buffer_(buffer) {}

    BoolArrayListV(std::vector<int> *buffer, uint32_t start, uint32_t end)
        : start_(start), end_(end), buffer_(buffer) {}

    ~BoolArrayListV() {}

    std::unique_ptr<ConstIterator<uint64_t, bool>> GetIterator() override {
        return std::unique_ptr<BoolArrayListIterator>(
            new BoolArrayListIterator(buffer_, start_, end_));
    }
    ConstIterator<uint64_t, bool> *GetRawIterator() override {
        return new BoolArrayListIterator(buffer_, start_, end_);
    }
    virtual const uint64_t GetCount() { return end_ - start_; }
    virtual bool At(uint64_t pos) const { return buffer_->at(start_ + pos); }

 protected:
    uint64_t start_;
    uint64_t end_;
    std::vector<int> *buffer_;
};

template <class V>
class InnerRowsIterator : public ConstIterator<uint64_t, V> {
 public:
    InnerRowsIterator(ListV<V> *list, uint64_t start, uint64_t end)
        : ConstIterator<uint64_t, V>(),
          root_(list->GetIterator()),
          pos_(0),
          start_(start),
          end_(end) {
        if (nullptr != root_) {
            SeekToFirst();
        }
    }
    ~InnerRowsIterator() {}
    virtual bool Valid() const {
        return root_->Valid() && pos_ <= end_ && pos_ >= start_;
    }
    virtual void Next() {
        pos_++;
        return root_->Next();
    }
    virtual const uint64_t &GetKey() const { return root_->GetKey(); }
    virtual const V &GetValue() { return root_->GetValue(); }
    virtual void Seek(const uint64_t &k) { root_->Seek(k); }
    virtual void SeekToFirst() {
        root_->SeekToFirst();
        pos_ = 0;
        while (root_->Valid() && pos_ < start_) {
            root_->Next();
            pos_++;
        }
    }
    bool IsSeekable() const { return root_->IsSeekable(); }
    std::unique_ptr<ConstIterator<uint64_t, V>> root_;
    uint64_t pos_;
    const uint64_t start_;
    const uint64_t end_;
};

template <class V>
class InnerRowsRangeIterator : public ConstIterator<uint64_t, V> {
 public:
    InnerRowsRangeIterator(std::unique_ptr<ConstIterator<uint64_t, V>> &&iter, uint64_t start, uint64_t end)
        : ConstIterator<uint64_t, V>(), root_(std::move(iter)), pos_(0), start_rows_(start), end_range_(end) {
        SeekToFirst();
    }
    ~InnerRowsRangeIterator() override {}

    bool Valid() const override {
        return root_->Valid() && pos_ >= start_rows_ && root_->GetKey() >= end_range_;
    }

    void Next() override {
        pos_++;
        return root_->Next();
    }
    const uint64_t &GetKey() const override { return root_->GetKey(); }
    const V &GetValue() override { return root_->GetValue(); }
    void Seek(const uint64_t &k) override { root_->Seek(k); }
    void SeekToFirst() override {
        root_->SeekToFirst();
        pos_ = 0;
        while (root_->Valid() && pos_ < start_rows_) {
            root_->Next();
            pos_++;
        }
    }
    bool IsSeekable() const { return root_->IsSeekable(); }

    std::unique_ptr<ConstIterator<uint64_t, V>> root_;
    uint64_t pos_;
    const uint64_t start_rows_;
    const uint64_t end_range_;
};

template <class V>
class InnerRangeIterator : public ConstIterator<uint64_t, V> {
 public:
    InnerRangeIterator(ListV<V> *list, uint64_t start, uint64_t end)
        : ConstIterator<uint64_t, V>(),
          root_(list->GetIterator()),
          start_key_(0),
          start_(start),
          end_(end) {
        if (nullptr != root_) {
            SeekToFirst();
            start_key_ = root_->Valid() ? root_->GetKey() : 0;
        }
    }
    ~InnerRangeIterator() {}
    virtual bool Valid() const {
        return root_->Valid() && root_->GetKey() <= start_ &&
               root_->GetKey() >= end_;
    }
    virtual void Next() { return root_->Next(); }
    virtual const uint64_t &GetKey() const { return root_->GetKey(); }
    virtual const V &GetValue() { return root_->GetValue(); }
    virtual void Seek(const uint64_t &k) { root_->Seek(k); }
    virtual void SeekToFirst() {
        root_->SeekToFirst();
        root_->Seek(start_);
    }
    virtual bool IsSeekable() const { return root_->IsSeekable(); }

    std::unique_ptr<ConstIterator<uint64_t, V>> root_;
    // the row key corresponding to the window
    uint64_t start_key_;
    // range start, key decrease start to end
    const uint64_t start_;
    // range end, key decrease start to end
    const uint64_t end_;
};

template <class V>
class InnerRangeList : public ListV<V> {
 public:
    InnerRangeList(ListV<V> *root, uint64_t start, uint64_t end)
        : ListV<V>(), root_(root), start_(start), end_(end) {}
    virtual ~InnerRangeList() {}
    // TODO(chenjing): at 数组越界处理
    virtual std::unique_ptr<ConstIterator<uint64_t, V>> GetIterator() {
        return std::unique_ptr<InnerRangeIterator<V>>(
            new InnerRangeIterator<V>(root_, start_, end_));
    }
    virtual ConstIterator<uint64_t, V> *GetRawIterator() {
        return new InnerRangeIterator<V>(root_, start_, end_);
    }

    ListV<V> *root_;
    uint64_t start_;
    uint64_t end_;
};

template <class V>
class InnerRowsList : public ListV<V> {
 public:
    InnerRowsList(ListV<V> *root, uint64_t start, uint64_t end)
        : ListV<V>(), root_(root), start_(start), end_(end) {}
    virtual ~InnerRowsList() {}
    // TODO(chenjing): at 数组越界处理
    virtual std::unique_ptr<ConstIterator<uint64_t, V>> GetIterator() {
        return std::unique_ptr<InnerRowsIterator<V>>(
            new InnerRowsIterator<V>(root_, start_, end_));
    }
    virtual ConstIterator<uint64_t, V> *GetRawIterator() {
        return new InnerRowsIterator<V>(root_, start_, end_);
    }

    ListV<V> *root_;
    uint64_t start_;
    uint64_t end_;
};

// start as ROWS offset, end as RANGE offset
// |end_range ... start_rows| .. current row|
// |<--------  iterator goes
template <class V>
class InnerRowsRangeList : public ListV<V> {
 public:
    InnerRowsRangeList(ListV<V> *root, uint64_t start_rows, uint64_t end_range)
        : ListV<V>(), root_(root), start_rows_(start_rows), end_range_(end_range) {}
    ~InnerRowsRangeList() override {}

    std::unique_ptr<ConstIterator<uint64_t, V>> GetIterator() override {
        return std::make_unique<InnerRowsRangeIterator<V>>(root_->GetIterator(), start_rows_, end_range_);
    }
    ConstIterator<uint64_t, V> *GetRawIterator() override {
        return new InnerRowsRangeIterator<V>(root_->GetIterator(), start_rows_, end_range_);
    }

    ListV<V> *root_;
    uint64_t start_rows_;
    uint64_t end_range_;
};

template <class V>
class ColumnIterator : public ConstIterator<uint64_t, V> {
 public:
    ColumnIterator(std::unique_ptr<RowIterator> &&it, const ColumnImpl<V> *column_impl)
        : ConstIterator<uint64_t, V>(), row_iter_(std::move(it)), column_impl_(column_impl) {
        if (row_iter_ != nullptr) {
            row_iter_->SeekToFirst();
        }
    }
    ~ColumnIterator() {}
    void Seek(const uint64_t &key) override { row_iter_->Seek(key); }
    void SeekToFirst() override { row_iter_->SeekToFirst(); }
    bool Valid() const override { return row_iter_->Valid(); }
    void Next() override { row_iter_->Next(); }
    const V &GetValue() override {
        value_ = column_impl_->GetFieldUnsafe(row_iter_->GetValue());
        return value_;
    }
    bool IsValueNull() override {
        return column_impl_->IsNull(row_iter_->GetValue());
    }
    const uint64_t &GetKey() const override { return row_iter_->GetKey(); }
    bool IsSeekable() const override { return row_iter_->IsSeekable(); }

 protected:
    std::unique_ptr<RowIterator> row_iter_;
    const ColumnImpl<V> *column_impl_;
    V value_;
};

template <class V>
class NonNullColumnIterator : public ColumnIterator<V> {
 public:
    NonNullColumnIterator(std::unique_ptr<RowIterator> &&it, const ColumnImpl<V> *column_impl)
    : ColumnIterator<V>(std::move(it), column_impl), valid_(true) {
        NextNonNull();
    }
    ~NonNullColumnIterator() override {}

    void Seek(const uint64_t &key) override {
        valid_ = true;
        this->row_iter_->Seek(key);
        NextNonNull(true);
    }

    void SeekToFirst() override {
        valid_ = true;
        this->row_iter_->SeekToFirst();
        NextNonNull();
    }
    bool Valid() const override { return valid_ && this->row_iter_->Valid(); }

    void Next() override {
        this->row_iter_->Next();
        NextNonNull();
    }

 private:
    void NextNonNull(bool current_key_only = false) {
        if (!this->row_iter_ || !this->row_iter_->Valid()) {
            return;
        }

        auto key = this->row_iter_->GetKey();

        while (this->row_iter_->Valid()) {
            bool is_null = true;
            V val {};
            this->column_impl_->GetField(this->row_iter_->GetValue(), &val, &is_null);
            if (!is_null) {
                break;
            }

            if (current_key_only && key != this->row_iter_->GetKey()) {
                // invalid if key not seeked
                valid_ = false;
                break;
            }

            this->row_iter_->Next();
        }
    }

 private:
    bool valid_;
};

// column list filters out NULL values
template <class V>
class NonNullColumnList : public ListV<V> {
 public:
    explicit NonNullColumnList(ColumnImpl<V> *impl) : columns_(impl) {}
    ~NonNullColumnList() override {}

    ConstIterator<uint64_t, V> *GetRawIterator() override {
        return new NonNullColumnIterator<V>(columns_->root()->GetIterator(), columns_);
    }

 private:
    ColumnImpl<V> *columns_;
};

}  // namespace codec
}  // namespace hybridse

#endif  // HYBRIDSE_INCLUDE_CODEC_LIST_ITERATOR_CODEC_H_
