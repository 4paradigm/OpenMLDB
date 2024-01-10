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

#ifndef HYBRIDSE_INCLUDE_VM_MEM_CATALOG_H_
#define HYBRIDSE_INCLUDE_VM_MEM_CATALOG_H_

#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include "glog/logging.h"
#include "vm/catalog.h"

namespace hybridse {
namespace vm {

using hybridse::codec::Row;
using hybridse::codec::RowIterator;
using hybridse::codec::WindowIterator;

struct AscKeyComparor {
    bool operator()(std::pair<std::string, Row> i,
                    std::pair<std::string, Row> j) {
        return i.first < j.first;
    }
};
struct AscComparor {
    bool operator()(std::pair<uint64_t, Row> i, std::pair<uint64_t, Row> j) {
        return i.first < j.first;
    }
};

struct DescComparor {
    bool operator()(std::pair<uint64_t, Row> i, std::pair<uint64_t, Row> j) {
        return i.first > j.first;
    }
};

typedef std::deque<std::pair<uint64_t, Row>> MemTimeTable;
typedef std::vector<Row> MemTable;
typedef std::map<std::string, MemTimeTable, std::greater<std::string>>
    MemSegmentMap;

class MemTimeTableIterator : public RowIterator {
 public:
    MemTimeTableIterator(const MemTimeTable* table, const vm::Schema* schema);
    MemTimeTableIterator(const MemTimeTable* table, const vm::Schema* schema,
                         int32_t start, int32_t end);
    ~MemTimeTableIterator();
    void Seek(const uint64_t& ts) override;
    void SeekToFirst() override;
    const uint64_t& GetKey() const override;
    void Next() override;
    bool Valid() const override;
    const Row& GetValue() override;
    bool IsSeekable() const override;

 private:
    const MemTimeTable* table_;
    const Schema* schema_;
    const MemTimeTable::const_iterator start_iter_;
    const MemTimeTable::const_iterator end_iter_;
    MemTimeTable::const_iterator iter_;
};

class MemTableIterator : public RowIterator {
 public:
    MemTableIterator(const MemTable* table, const vm::Schema* schema);
    MemTableIterator(const MemTable* table, const vm::Schema* schema,
                     int32_t start, int32_t end);
    ~MemTableIterator();
    void Seek(const uint64_t& ts) override;
    void SeekToFirst() override;
    const uint64_t& GetKey() const override;
    const Row& GetValue() override;
    void Next() override;
    bool Valid() const override;
    bool IsSeekable() const override;

 private:
    const MemTable* table_;
    const Schema* schema_;
    const MemTable::const_iterator start_iter_;
    const MemTable::const_iterator end_iter_;
    MemTable::const_iterator iter_;
    uint64_t key_;
};

class MemWindowIterator : public WindowIterator {
 public:
    MemWindowIterator(const MemSegmentMap* partitions, const Schema* schema);

    ~MemWindowIterator();

    void Seek(const std::string& key);
    void SeekToFirst();
    void Next();
    bool Valid();
    RowIterator* GetRawValue();
    const Row GetKey();

 private:
    const MemSegmentMap* partitions_;
    const Schema* schema_;
    MemSegmentMap::const_iterator iter_;
    const MemSegmentMap::const_iterator start_iter_;
    const MemSegmentMap::const_iterator end_iter_;
};

class MemRowHandler : public RowHandler {
 public:
    explicit MemRowHandler(const Row row)
        : RowHandler(), table_name_(""), db_(""), schema_(nullptr), row_(row) {}
    MemRowHandler(const Row row, const vm::Schema* schema)
        : RowHandler(), table_name_(""), db_(""), schema_(schema), row_(row) {}
    ~MemRowHandler() {}

    const Schema* GetSchema() override { return schema_; }
    const std::string& GetName() override { return table_name_; }
    const std::string& GetDatabase() override { return db_; }
    const Row& GetValue() override { return row_; }
    const std::string GetHandlerTypeName() override { return "MemRowHandler"; }

 private:
    std::string table_name_;
    std::string db_;
    const Schema* schema_;
    Row row_;
};

class MemTableHandler : public TableHandler {
 public:
    MemTableHandler();
    explicit MemTableHandler(const Schema* schema);
    MemTableHandler(const std::string& table_name, const std::string& db,
                    const Schema* schema);
    ~MemTableHandler() override;

    const Types& GetTypes() override { return types_; }
    const Schema* GetSchema() override { return schema_; }
    const std::string& GetName() override { return table_name_; }
    const IndexHint& GetIndex() override { return index_hint_; }
    const std::string& GetDatabase() override { return db_; }

    RowIterator* GetRawIterator() override;

    void AddRow(const Row& row);
    void Reverse();
    const uint64_t GetCount() override { return table_.size(); }
    Row At(uint64_t pos) override {
        return pos < table_.size() ? table_.at(pos) : Row();
    }

    const OrderType GetOrderType() const override { return order_type_; }
    void SetOrderType(const OrderType order_type) { order_type_ = order_type; }
    const std::string GetHandlerTypeName() override {
        return "MemTableHandler";
    }

 protected:
    void Resize(const size_t size);
    bool SetRow(const size_t idx, const Row& row);
    const std::string table_name_;
    const std::string db_;
    const Schema* schema_;
    Types types_;
    IndexHint index_hint_;
    MemTable table_;
    OrderType order_type_;
};

class MemTimeTableHandler : public TableHandler {
 public:
    MemTimeTableHandler();
    explicit MemTimeTableHandler(const Schema* schema);
    MemTimeTableHandler(const std::string& table_name, const std::string& db,
                        const Schema* schema);
    const Types& GetTypes() override;
    ~MemTimeTableHandler() override;
    const Schema* GetSchema() override { return schema_; }
    const std::string& GetName() override { return table_name_; }
    const IndexHint& GetIndex() override { return index_hint_; }
    RowIterator* GetRawIterator() override;
    const std::string& GetDatabase() override { return db_; }
    void AddRow(const uint64_t key, const Row& v);
    void AddFrontRow(const uint64_t key, const Row& v);
    void PopBackRow();
    void PopFrontRow();
    virtual const std::pair<uint64_t, Row>& GetFrontRow() {
        return table_.front();
    }
    virtual const std::pair<uint64_t, Row>& GetBackRow() {
        return table_.back();
    }
    void Sort(const bool is_asc);
    void Reverse();
    const uint64_t GetCount() override { return table_.size(); }
    Row At(uint64_t pos) override {
        return pos < table_.size() ? table_.at(pos).second : Row();
    }
    void SetOrderType(const OrderType order_type) { order_type_ = order_type; }
    const OrderType GetOrderType() const override { return order_type_; }
    const std::string GetHandlerTypeName() override {
        return "MemTimeTableHandler";
    }

 protected:
    const std::string table_name_;
    const std::string db_;
    const Schema* schema_;
    Types types_;
    IndexHint index_hint_;
    MemTimeTable table_;
    OrderType order_type_;
};

class Window : public MemTimeTableHandler {
 public:
    enum WindowFrameType {
        kFrameRows,
        kFrameRowsRange,
        kFrameRowsMergeRowsRange
    };
    Window() : MemTimeTableHandler() {}
    virtual ~Window() {}

    std::unique_ptr<RowIterator> GetIterator() override {
        return std::make_unique<vm::MemTimeTableIterator>(&table_, schema_);
    }

    RowIterator* GetRawIterator() override { return new vm::MemTimeTableIterator(&table_, schema_); }
    virtual bool BufferData(uint64_t key, const Row& row) = 0;
    virtual void PopBackData() { PopBackRow(); }
    virtual void PopFrontData() = 0;

    const std::string GetHandlerTypeName() override { return "Window"; }

    bool instance_not_in_window() const { return instance_not_in_window_; }
    void set_instance_not_in_window(bool flag) { instance_not_in_window_ = flag; }

    bool exclude_current_time() const { return exclude_current_time_; }
    void set_exclude_current_time(bool flag) { exclude_current_time_ = flag; }

    bool without_order_by() const { return without_order_by_; }
    void set_without_order_by(bool flag) { without_order_by_ = flag; }

 protected:
    bool exclude_current_time_ = false;
    bool instance_not_in_window_ = false;
    bool without_order_by_ = false;
};
class WindowRange {
 public:
    enum WindowPositionStatus {
        kInWindow,
        kExceedWindow,
        kBeforeWindow,
    };

    WindowRange()
        : frame_type_(Window::kFrameRows),
          start_offset_(0),
          end_offset_(0),
          start_row_(0),
          end_row_(0),
          max_size_(0) {}
    WindowRange(Window::WindowFrameType frame_type, int64_t start_offset,
                int64_t end_offset, uint64_t rows_preceding, uint64_t max_size)
        : frame_type_(frame_type),
          start_offset_(start_offset),
          end_offset_(end_offset),
          start_row_(rows_preceding),
          end_row_(0),
          max_size_(max_size) {}

    virtual ~WindowRange() {}

    static WindowRange CreateRowsWindow(uint64_t rows_preceding) {
        return WindowRange(Window::kFrameRows, 0, 0, rows_preceding, 0);
    }
    static WindowRange CreateRowsRangeWindow(int64_t start_offset,
                                             int64_t end_offset,
                                             uint64_t max_size = 0) {
        return WindowRange(Window::kFrameRowsRange, start_offset, end_offset, 0,
                           max_size);
    }
    static WindowRange CreateRowsMergeRowsRangeWindow(int64_t start_offset,
                                                      uint64_t rows_preceding,
                                                      uint64_t max_size = 0) {
        return WindowRange(Window::kFrameRowsMergeRowsRange, start_offset, 0,
                           rows_preceding, max_size);
    }
    const WindowPositionStatus GetWindowPositionStatus(
        bool out_of_rows, bool before_window, bool exceed_window) const {
        switch (frame_type_) {
            case Window::WindowFrameType::kFrameRows:
                return out_of_rows ? kExceedWindow : (before_window ? kBeforeWindow : kInWindow);
            case Window::WindowFrameType::kFrameRowsMergeRowsRange: {
                return out_of_rows
                           ? (before_window
                                  ? kBeforeWindow
                                  : exceed_window ? kExceedWindow : kInWindow)
                           : before_window ? kBeforeWindow : kInWindow;
            }
            case Window::WindowFrameType::kFrameRowsRange:
                return exceed_window
                           ? kExceedWindow
                           : before_window ? kBeforeWindow : kInWindow;
            default:
                return kExceedWindow;
        }
        return kExceedWindow;
    }

    Window::WindowFrameType frame_type_;
    int64_t start_offset_;
    int64_t end_offset_;
    uint64_t start_row_;
    uint64_t end_row_;
    uint64_t max_size_;
};

/**
 * |start_ts.............end_ts|                current_ts|
 * |.............history window|    current history buffer|
 */
class HistoryWindow : public Window {
 public:
    explicit HistoryWindow(const WindowRange& window_range)
        : Window(), window_range_(window_range), current_history_buffer_() {}
    ~HistoryWindow() {}

    void PopFrontData() override {
        if (current_history_buffer_.empty()) {
            PopFrontRow();
        } else {
            current_history_buffer_.pop_front();
        }
    }

    virtual void PopEffectiveDataIfAny() {
        if (!table_.empty()) {
            PopFrontRow();
        }
    }
    bool BufferData(uint64_t key, const Row& row) override;

    // add newer row into window
    bool BufferDataImpl(uint64_t key, const Row& row);

 protected:
    bool BufferCurrentHistoryBuffer(uint64_t key, const Row& row, uint64_t end_ts);

    // sliding rows data from `current_history_buffer_` into effective window
    // by giving the new start_ts and end_ts.
    // Resulting the new effective window data whose bound is [start_ts, end_ts],
    // NOTE
    // - window bounds should be greater or equal to 0, < 0 is not supported yet,
    // - values greater than int64_max is not considered as well
    // - start_ts_inclusive > end_ts_inclusive is expected for rows window, e.g.
    //   `(rows between .. and current_row exclude current_time)`.
    //   Absolutely confusing design though, should refactored later
    // TODO(ace): note above
    //
    // - elements in `current_history_buffer_` that `ele.first <= end_ts` goes out of
    //   `current_history_buffer_` and pushed into effective window
    // - elements in effective window where `ele.first < start_ts` goes out of effective window
    //
    // `start_ts_inclusive` and `end_ts_inclusive` can be empty, which effectively means less than 0.
    // if `start_ts_inclusive` is empty, no rows goes out of effective window
    // if `end_ts_inclusive` is empty, no rows goes out of history buffer and into effective window
    void SlideWindow(std::optional<uint64_t> start_ts_inclusive, std::optional<uint64_t> end_ts_inclusive);

    // push the row to the start of window
    // - pop last elements in window if exceed max window size
    // - also pop last elements in window if there ts less than `start_ts`
    //
    // if `start_ts` is empty, no rows eliminated from window
    bool BufferEffectiveWindow(uint64_t key, const Row& row, std::optional<uint64_t> start_ts);

    bool Slide(std::optional<uint64_t> start_ts);

    bool BufferCurrentTimeBuffer(uint64_t key, const Row& row, uint64_t start_ts);

    WindowRange window_range_;
    MemTimeTable current_history_buffer_;
};

/**
 * |start_ts....................................current_ts|
 * |................................current history window|
 * current history window is effective window
 * there is no current_history_buffer_
 */
class CurrentHistoryWindow : public HistoryWindow {
 public:
    explicit CurrentHistoryWindow(const WindowRange& window_range)
        : HistoryWindow(window_range) {}
    explicit CurrentHistoryWindow(const Window::WindowFrameType window_frame,
                                  uint64_t start_offset, uint64_t max_size)
        : HistoryWindow(
              WindowRange(window_frame, start_offset, 0, 0, max_size)) {}
    explicit CurrentHistoryWindow(const Window::WindowFrameType window_frame,
                                  uint64_t start_offset, uint64_t start_rows,
                                  uint64_t max_size)
        : HistoryWindow(WindowRange(window_frame, start_offset, 0, start_rows,
                                    max_size)) {}
    ~CurrentHistoryWindow() {}

    void PopFrontData() override { PopFrontRow(); }

    bool BufferData(uint64_t key, const Row& row) override;
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<MemTimeTableHandler>>>
    MemTables;
typedef std::map<std::string, std::shared_ptr<type::Database>> Databases;

class MemSegmentHandler : public TableHandler {
 public:
    MemSegmentHandler(std::shared_ptr<PartitionHandler> partition_hander,
                      const std::string& key)
        : partition_hander_(partition_hander), key_(key) {}

    virtual ~MemSegmentHandler() {}

    const vm::Schema* GetSchema() override {
        return partition_hander_->GetSchema();
    }

    const std::string& GetName() override { return partition_hander_->GetName(); }

    const std::string& GetDatabase() override {
        return partition_hander_->GetDatabase();
    }

    const vm::Types& GetTypes() override { return partition_hander_->GetTypes(); }

    const vm::IndexHint& GetIndex() override {
        return partition_hander_->GetIndex();
    }

    const OrderType GetOrderType() const override {
        return partition_hander_->GetOrderType();
    }
    RowIterator* GetRawIterator() override {
        auto iter = partition_hander_->GetWindowIterator();
        if (iter) {
            iter->Seek(key_);
            return iter->Valid() ? iter->GetRawValue() : nullptr;
        }
        return nullptr;
    }
    std::unique_ptr<vm::WindowIterator> GetWindowIterator(const std::string& idx_name) override {
        LOG(WARNING) << "SegmentHandler can't support window iterator";
        return std::unique_ptr<WindowIterator>();
    }
    const uint64_t GetCount() override {
        auto iter = GetIterator();
        if (!iter) {
            return 0;
        }
        iter->SeekToFirst();
        uint64_t cnt = 0;
        while (iter->Valid()) {
            cnt++;
            iter->Next();
        }
        return cnt;
    }
    Row At(uint64_t pos) override {
        auto iter = GetIterator();
        if (!iter) {
            return Row();
        }
        iter->SeekToFirst();
        while (pos-- > 0 && iter->Valid()) {
            iter->Next();
        }
        return iter->Valid() ? iter->GetValue() : Row();
    }
    const std::string GetHandlerTypeName() override {
        return "MemSegmentHandler";
    }

 private:
    std::shared_ptr<vm::PartitionHandler> partition_hander_;
    std::string key_;
};

class MemPartitionHandler : public PartitionHandler, public std::enable_shared_from_this<PartitionHandler> {
 public:
    MemPartitionHandler();
    explicit MemPartitionHandler(const Schema* schema);
    MemPartitionHandler(const std::string& table_name, const std::string& db,
                        const Schema* schema);

    ~MemPartitionHandler();
    const Types& GetTypes() override;
    const IndexHint& GetIndex() override;
    const Schema* GetSchema() override;
    const std::string& GetName() override;
    const std::string& GetDatabase() override;
    RowIterator* GetRawIterator() override { return nullptr; }
    std::unique_ptr<WindowIterator> GetWindowIterator() override;
    bool AddRow(const std::string& key, uint64_t ts, const Row& row);
    void Sort(const bool is_asc);
    void Reverse();
    void Print();
    const uint64_t GetCount() override { return partitions_.size(); }
    std::shared_ptr<TableHandler> GetSegment(const std::string& key) override {
        return std::shared_ptr<MemSegmentHandler>(
            new MemSegmentHandler(shared_from_this(), key));
    }
    void SetOrderType(const OrderType order_type) { order_type_ = order_type; }
    const OrderType GetOrderType() const override { return order_type_; }
    const std::string GetHandlerTypeName() override {
        return "MemPartitionHandler";
    }

 private:
    std::string table_name_;
    std::string db_;
    const Schema* schema_;
    MemSegmentMap partitions_;
    Types types_;
    IndexHint index_hint_;
    OrderType order_type_;
};

class ConcatTableHandler : public MemTimeTableHandler {
 public:
    ConcatTableHandler(std::shared_ptr<TableHandler> left, size_t left_slices,
                       std::shared_ptr<TableHandler> right, size_t right_slices)
        : MemTimeTableHandler(),
          status_(base::Status::Running()),
          left_(left),
          left_slices_(left_slices),
          right_(right),
          right_slices_(right_slices) {}
    ~ConcatTableHandler() {}
    Row At(uint64_t pos) override {
        if (!status_.isRunning()) {
            return MemTimeTableHandler::At(pos);
        }
        status_ = SyncValue();
        return MemTimeTableHandler::At(pos);
    }
    RowIterator* GetRawIterator() override {
        if (status_.isRunning()) {
            status_ = SyncValue();
        }
        return MemTimeTableHandler::GetRawIterator();
    }
    const uint64_t GetCount() override {
        if (status_.isRunning()) {
            status_ = SyncValue();
        }
        return MemTimeTableHandler::GetCount();
    }

 private:
    base::Status SyncValue() {
        DLOG(INFO) << "Sync... concat left table and right table";
        if (!left_) {
            return base::Status::OK();
        }
        auto left_iter = left_->GetIterator();
        if (!left_iter) {
            return base::Status::OK();
        }

        auto right_iter = std::unique_ptr<RowIterator>();
        if (right_) {
            right_iter = right_->GetIterator();
        }
        left_iter->SeekToFirst();
        while (left_iter->Valid()) {
            if (!right_iter || !right_iter->Valid()) {
                AddRow(left_iter->GetKey(),
                       Row(left_slices_, left_iter->GetValue(), right_slices_,
                           Row()));
            } else {
                AddRow(left_iter->GetKey(),
                       Row(left_slices_, left_iter->GetValue(), right_slices_,
                           right_iter->GetValue()));
                right_iter->Next();
            }
            left_iter->Next();
        }
        return base::Status::OK();
    }
    base::Status status_;
    std::shared_ptr<TableHandler> left_;
    size_t left_slices_;
    std::shared_ptr<TableHandler> right_;
    size_t right_slices_;
};

class MemCatalog : public Catalog {
 public:
    MemCatalog();

    ~MemCatalog();

    bool Init();

    std::shared_ptr<type::Database> GetDatabase(const std::string& db) override {
        return dbs_[db];
    }
    std::shared_ptr<TableHandler> GetTable(const std::string& db,
                                           const std::string& table_name) override {
        return tables_[db][table_name];
    }
    bool IndexSupport() override { return true; }

 private:
    MemTables tables_;
    Databases dbs_;
};

/**
 * Result table handler specified for request union:
 * (1) The first row is fixed to be the request row
 * (2) O(1) time construction
 */
class RequestUnionTableHandler : public TableHandler {
 public:
    RequestUnionTableHandler(uint64_t request_ts, const Row& request_row,
                             const std::shared_ptr<TableHandler>& window)
        : request_ts_(request_ts), request_row_(request_row), window_(window) {}
    ~RequestUnionTableHandler() {}

    RowIterator* GetRawIterator() override;

    const Types& GetTypes() override { return window_->GetTypes(); }
    const IndexHint& GetIndex() override { return window_->GetIndex(); }
    const OrderType GetOrderType() const override { return window_->GetOrderType(); }
    const Schema* GetSchema() override { return window_->GetSchema(); }
    const std::string& GetName() override { return window_->GetName(); }
    const std::string& GetDatabase() override { return window_->GetDatabase(); }

 private:
    uint64_t request_ts_;
    const Row request_row_;
    std::shared_ptr<TableHandler> window_;
};

// row iter interfaces for llvm
void GetRowIter(int8_t* input, int8_t* iter);
bool RowIterHasNext(int8_t* iter);
void RowIterNext(int8_t* iter);
int8_t* RowIterGetCurSlice(int8_t* iter, size_t idx);
size_t RowIterGetCurSliceSize(int8_t* iter, size_t idx);
void RowIterDelete(int8_t* iter);
int8_t* RowGetSlice(int8_t* row_ptr, size_t idx);
size_t RowGetSliceSize(int8_t* row_ptr, size_t idx);
}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_VM_MEM_CATALOG_H_
