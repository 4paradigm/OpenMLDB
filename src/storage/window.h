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
#include <cstdint>
#include <iostream>
#include <vector>
#include "storage/type_native_fn.h"
namespace fesql {
namespace storage {
template <class V>
class IteratorImpl;

struct Row {
    int8_t *buf;
    size_t size;
};
enum BOUND { UNBOUND, CURRENT, NORMAL };

template <class V>
class ListV {
 public:
    ListV() : start_(0), end_(0), buffer_({}) {}
    explicit ListV(const std::vector<V> &buffer)
        : start_(0), end_(buffer.size()), buffer_(buffer) {}
    ListV(const std::vector<V> &buffer, uint32_t start, uint32_t end)
        : start_(start), end_(end), buffer_(buffer) {}

    ~ListV() {}
    // TODO(chenjing): at 数组越界处理
    virtual const V At(int32_t pos) const { return buffer_.at(pos); }
    virtual const uint32_t Count() const { return end_ - start_; }
    virtual const uint32_t GetStart() const { return start_; }
    virtual const uint32_t GetEnd() const { return end_; }
    friend IteratorImpl<V>;

 protected:
    uint32_t start_;
    uint32_t end_;
    std::vector<V> buffer_;
};

template <class V, class R>
class WrapListImpl : public ListV<V> {
 public:
    explicit WrapListImpl(const ListV<R> &root) : ListV<V>(), root_(root) {}
    ~WrapListImpl() {}
    virtual const V GetField(R row) const = 0;
    virtual const V At(int32_t pos) const { return GetField(root_.At(pos)); }
    virtual const uint32_t Count() const { return root_.Count(); }
    virtual const uint32_t GetStart() const { return root_.GetStart(); }
    virtual const uint32_t GetEnd() const { return root_.GetEnd(); }

 protected:
    ListV<R> root_;
};

template <class V>
class ColumnImpl : public WrapListImpl<V, Row> {
 public:
    ColumnImpl(const ListV<Row> &impl, uint32_t offset)
        : WrapListImpl<V, Row>(impl), offset_(offset) {}
    const V GetField(const Row row) const {
        V value;
        const int8_t *ptr = row.buf + offset_;
        value = *((const V *)ptr);
        return value;
    }
    friend IteratorImpl<V>;

 private:
    uint32_t offset_;
};

class StringColumnImpl : public ColumnImpl<fesql::storage::StringRef> {
 public:
    StringColumnImpl(const ListV<Row> &impl, int32_t str_field_offset,
                     int32_t next_str_field_offset, int32_t str_start_offset)
        : ColumnImpl<::fesql::storage::StringRef>(impl, 0u),
          str_field_offset_(str_field_offset),
          next_str_field_offset_(next_str_field_offset),
          str_start_offset_(str_start_offset) {}
    const StringRef GetField(Row row) const override {
        int32_t addr_space = fesql::storage::v1::GetAddrSpace(row.size);
        fesql::storage::StringRef value;
        fesql::storage::v1::GetStrField(
            row.buf, str_field_offset_, next_str_field_offset_,
            str_start_offset_, addr_space,
            reinterpret_cast<int8_t **>(&(value.data)), &(value.size));
        return value;
    }

 private:
    uint32_t str_field_offset_;
    uint32_t next_str_field_offset_;
    uint32_t str_start_offset_;
};

class Window : public ListV<Row> {
 public:
    Window(int64_t start_offset, int64_t end_offset)
        : ListV<Row>(),
          start_offset_(start_offset),
          end_offset_(end_offset),
          max_size_(0),
          keys_({}) {}
    Window(int64_t start_offset, int64_t end_offset, uint32_t max_size)
        : ListV<Row>(),
          start_offset_(start_offset),
          end_offset_(end_offset),
          max_size_(max_size),
          keys_({}) {}
    virtual ~Window() {}
    virtual void BufferData(uint64_t key, const Row &row) = 0;
    void Reserve(uint64_t size) {
        buffer_.reserve(size);
        keys_.reserve(size);
    }

 protected:
    int64_t start_offset_;
    int32_t end_offset_;
    uint32_t max_size_;
    std::vector<uint64_t> keys_;
};

class SlideWindow : public ListV<Row> {
 public:
    SlideWindow(int64_t start_offset, int64_t end_offset,
                const std::vector<Row> &buffer,
                const std::vector<uint64_t> &keys, uint32_t start)
        : ListV<Row>(buffer, start, start),
          start_offset_(start_offset),
          end_offset_(end_offset),
          max_size_(0),
          keys_(keys) {
        window_end_ = buffer.size();
    }
    SlideWindow(int64_t start_offset, int64_t end_offset, uint32_t max_size,
                const std::vector<Row> &buffer,
                const std::vector<uint64_t> &keys, uint32_t start)
        : ListV<Row>(buffer, start, start),
          start_offset_(start_offset),
          end_offset_(end_offset),
          max_size_(max_size),
          keys_(keys) {
        window_end_ = buffer.size();
    }
    virtual ~SlideWindow() {}
    virtual bool Slide() = 0;

 protected:
    int64_t start_offset_;
    int32_t end_offset_;
    uint32_t max_size_;
    uint32_t window_end_;
    const std::vector<uint64_t> keys_;
};

// TODO(chenjing):
// 可以用一个vector引用初始化window，然后提供一个slide接口，只是滑动窗口边界。
// 历史窗口，窗口内数据从历史某个时刻记录到当前记录
class CurrentHistoryWindow : public Window {
 public:
    explicit CurrentHistoryWindow(int64_t start_offset)
        : Window(start_offset, 0) {}
    CurrentHistoryWindow(int64_t start_offset, uint32_t max_size)
        : Window(start_offset, 0, max_size) {}

    void BufferData(uint64_t key, const Row &row) {
        keys_.push_back(key);
        buffer_.push_back(row);
        end_++;
        int64_t sub = (key + start_offset_);
        uint64_t start_ts = sub < 0 ? 0u : static_cast<uint64_t>(sub);
        while (start_ < end_ &&
               ((0 != max_size_ && (end_ - start_) > max_size_) ||
                keys_[start_] <= start_ts)) {
            start_++;
        }
    }
};

// 历史无限窗口，窗口内数据包含所有历史到当前记录
class CurrentHistoryUnboundWindow : public Window {
 public:
    CurrentHistoryUnboundWindow() : Window(INT64_MIN, 0) {}
    explicit CurrentHistoryUnboundWindow(uint32_t max_size)
        : Window(INT64_MIN, 0, max_size) {}
    void BufferData(uint64_t key, const Row &row) {
        keys_.push_back(key);
        buffer_.push_back(row);
        end_++;
        while ((0 != max_size_ && (end_ - start_) > max_size_)) {
            start_++;
        }
    }
};

// TODO(chenjing):
// 可以用一个vector引用初始化window，然后提供一个slide接口，只是滑动窗口边界。
// 历史滑动窗口，窗口内数据从历史某个时刻记录到当前记录
class CurrentHistorySlideWindow : public SlideWindow {
 public:
    CurrentHistorySlideWindow(int64_t start_offset,
                              const std::vector<Row> &buffer,
                              const std::vector<uint64_t> &keys, uint32_t start)
        : SlideWindow(start_offset, 0, buffer, keys, start) {}
    CurrentHistorySlideWindow(int64_t start_offset, uint32_t max_size,
                              const std::vector<Row> &buffer,
                              const std::vector<uint64_t> &keys, uint32_t start)
        : SlideWindow(start_offset, 0, max_size, buffer, keys, start) {}
    bool Slide() {
        if (end_ >= window_end_) {
            return false;
        }
        uint64_t key = keys_[end_];
        end_ += 1;
        int64_t sub = (key + start_offset_);
        uint64_t start_ts = sub < 0 ? 0u : static_cast<uint64_t>(sub);
        while (start_ < end_ &&
               ((0 != max_size_ && (end_ - start_) > max_size_) ||
                keys_[start_] <= start_ts)) {
            start_++;
        }
        return true;
    }
};

// 历史无限滑动窗口，窗口内数据包含所有历史到当前记录
class CurrentHistoryUnboundSlideWindow : public SlideWindow {
 public:
    CurrentHistoryUnboundSlideWindow(const std::vector<Row> &buffer,
                                     const std::vector<uint64_t> &keys,
                                     uint32_t start)
        : SlideWindow(INT64_MIN, 0, buffer, keys, start) {}

    CurrentHistoryUnboundSlideWindow(uint32_t max_size,
                                     const std::vector<Row> &buffer,
                                     const std::vector<uint64_t> &keys,
                                     uint32_t start)
        : SlideWindow(INT64_MIN, 0, max_size, buffer, keys, start) {}
    bool Slide() {
        if (end_ >= window_end_) {
            return false;
        }
        end_ += 1;
        while ((0 != max_size_ && (end_ - start_) > max_size_)) {
            start_++;
        }
        return true;
    }
};

template <class V>
class IteratorV {
 public:
    IteratorV() {}
    virtual ~IteratorV() {}
    virtual const bool Valid() const = 0;
    virtual V Next() = 0;
    virtual void reset() = 0;
};

template <class V>
class IteratorImpl : public IteratorV<V> {
 public:
    explicit IteratorImpl(const ListV<V> &list)
        : list_(list),
          iter_start_(list.GetStart()),
          iter_end_(list.GetEnd()),
          pos_(list.GetStart()) {}

    explicit IteratorImpl(const IteratorImpl<V> &impl)
        : list_(impl.list_),
          iter_start_(impl.iter_start_),
          iter_end_(impl.iter_end_),
          pos_(impl.start_) {}

    IteratorImpl(const ListV<V> &list, int start, int end)
        : list_(list), iter_start_(start), iter_end_(end), pos_(start) {}

    ~IteratorImpl() {}
    const bool Valid() const { return pos_ < iter_end_; }

    V Next() { return list_.At(pos_++); }

    void reset() { pos_ = iter_start_; }

    IteratorImpl<V> *range(int start, int end) {
        if (start > end || end < iter_start_ || start > iter_end_) {
            return new IteratorImpl(list_, iter_start_, iter_start_);
        }
        start = start < iter_start_ ? iter_start_ : start;
        end = end > iter_end_ ? iter_end_ : end;
        return new IteratorImpl(list_, start, end);
    }

 protected:
    const ListV<V> &list_;
    const int iter_start_;
    const int iter_end_;
    int pos_;
};

typedef IteratorImpl<Row> WindowIteratorImpl;
typedef ListV<Row> WindowImpl;

}  // namespace storage
}  // namespace fesql

#endif  // SRC_STORAGE_WINDOW_H_
