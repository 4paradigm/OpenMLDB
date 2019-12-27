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
#include <forward_list>
#include <iostream>
#include <vector>
#include "storage/type_ir_builder.h"
namespace fesql {
namespace storage {
class WindowIteratorImpl;

struct Row {
    int8_t *buf;
    size_t size;
};
enum BOUND { UNBOUND, CURRENT, NORMAL };
/**
 * Window In Buffer
 * is window slide during a given buffer
 * buffer is a vector:
 *
 */
class Window {
 public:
    Window(int64_t start_offset, int64_t end_offset)
        : start_offset_(start_offset),
          end_offset_(end_offset),
          start_(0),
          end_(0),
          max_size_(0),
          buffer_({}),
          keys_({}) {}
    Window(int64_t start_offset, int64_t end_offset, uint32_t max_size)
        : start_offset_(start_offset),
          end_offset_(end_offset),
          start_(0),
          end_(0),
          max_size_(max_size),
          buffer_({}),
          keys_({}) {}
    const uint32_t Count() const { return end_ - start_; }
    virtual void BufferData(uint64_t key, const Row &row) = 0;
    virtual void SlideWindow() = 0;
    friend WindowIteratorImpl;
    void Reserve(uint64_t size) {
        buffer_.reserve(size);
        keys_.reserve(size);
    }

 protected:
    int64_t start_offset_;
    int32_t end_offset_;
    uint32_t start_;
    uint32_t end_;
    uint32_t max_size_;
    std::vector<Row> buffer_;
    std::vector<uint64_t> keys_;
};

/**
 * Window In Buffer
 * is window slide during a given buffer
 * buffer is a vector:
 *
 */
class SlideWindow {
 public:
    SlideWindow(int64_t start_offset, int64_t end_offset,
                std::vector<Row> &buffer, std::vector<uint64_t> &keys, uint32_t start)
        : start_offset_(start_offset),
          end_offset_(end_offset),
          start_(start),
          end_(start),
          max_size_(0),
          buffer_(buffer),
          keys_(keys) {
        window_end_ = buffer.size();
    }
    SlideWindow(int64_t start_offset, int64_t end_offset, uint32_t max_size,
                std::vector<Row> &buffer, std::vector<uint64_t> &keys, uint32_t start)
        : start_offset_(start_offset),
          end_offset_(end_offset),
          start_(start),
          end_(start),
          max_size_(max_size),
          buffer_(buffer),
          keys_(keys) {
        window_end_ = buffer.size();
    }
    const uint32_t Count() const { return end_ - start_; }
    virtual bool Slide() = 0;
    friend WindowIteratorImpl;

 protected:
    int64_t start_offset_;
    int32_t end_offset_;
    uint32_t start_;
    uint32_t end_;
    uint32_t max_size_;
    uint32_t window_end_;
    std::vector<Row> &buffer_;
    std::vector<uint64_t> &keys_;
};

// TODO(chenjing):
// 可以用一个vector引用初始化window，然后提供一个slide接口，只是滑动窗口边界。
/**
 * 历史窗口，窗口内数据从历史某个时刻记录到当前记录
 */
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
    void SlideWindow() {}
};

/**
 * 历史无限窗口，窗口内数据包含所有历史到当前记录
 */
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
    void SlideWindow() {}
};

// TODO(chenjing):
// 可以用一个vector引用初始化window，然后提供一个slide接口，只是滑动窗口边界。
/**
 * 历史滑动窗口，窗口内数据从历史某个时刻记录到当前记录
 */
class CurrentHistorySlideWindow : public SlideWindow {
 public:
    CurrentHistorySlideWindow(int64_t start_offset, std::vector<Row> &buffer,
                              std::vector<uint64_t> &keys, uint32_t start)
        : SlideWindow(start_offset, 0, buffer, keys, start) {}
    CurrentHistorySlideWindow(int64_t start_offset, uint32_t max_size,
                              std::vector<Row> &buffer,
                              std::vector<uint64_t> &keys, uint32_t start)
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

/**
 * 历史无限滑动窗口，窗口内数据包含所有历史到当前记录
 */
class CurrentHistoryUnboundSlideWindow : public SlideWindow {
 public:
    CurrentHistoryUnboundSlideWindow(std::vector<Row> &buffer,
                                     std::vector<uint64_t> &keys, uint32_t start)
        : SlideWindow(INT64_MIN, 0, buffer, keys, start) {}

    CurrentHistoryUnboundSlideWindow(uint32_t max_size,
                                     std::vector<Row> &buffer,
                                     std::vector<uint64_t> &keys, uint32_t start)
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
    IteratorImpl() : list_{}, start_(0), end_(0), pos_(0) {}
    explicit IteratorImpl(const std::vector<V> &list)
        : list_(list), start_(0), end_(list.size()), pos_(0) {}
    explicit IteratorImpl(const IteratorImpl<V> &impl)
        : list_(impl.list_),
          start_(impl.start_),
          end_(impl.end_),
          pos_(impl.start_) {}

    IteratorImpl(const std::vector<V> &list, int start, int end)
        : list_(list), start_(start), end_(end), pos_(start) {}
    ~IteratorImpl() {}
    const bool Valid() const { return pos_ < end_; }

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
    const std::vector<V> &list_;
    const int start_;
    const int end_;
    int pos_;
};
class WindowIteratorImpl : public IteratorImpl<Row> {
 public:
    explicit WindowIteratorImpl(const std::vector<Row> &list)
        : IteratorImpl<Row>(list) {}

    WindowIteratorImpl(const std::vector<Row> &list, int start, int end)
        : IteratorImpl<Row>(list, start, end) {}
    explicit WindowIteratorImpl(const Window &window)
        : IteratorImpl<Row>(window.buffer_, window.start_, window.end_) {}
    explicit WindowIteratorImpl(const SlideWindow &window)
        : IteratorImpl<Row>(window.buffer_, window.start_, window.end_) {}
    ~WindowIteratorImpl() {}
};

template <class V, class R>
class WrapIteratorImpl : public IteratorImpl<V> {
 public:
    explicit WrapIteratorImpl(const IteratorImpl<R> &root)
        : IteratorImpl<V>(), root_(root) {}

    ~WrapIteratorImpl() {}
    const bool Valid() const { return root_.Valid(); }

    V Next() { return GetField(root_.Next()); }
    virtual const V GetField(R row) const = 0;
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
    ColumnIteratorImpl(const IteratorImpl<Row> &impl, uint32_t offset)
        : WrapIteratorImpl<V, Row>(impl), offset_(offset) {}

    const V GetField(const Row row) const {
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

    const fesql::storage::StringRef GetField(Row row) const {
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

}  // namespace storage
}  // namespace fesql

#endif  // SRC_STORAGE_WINDOW_H_
