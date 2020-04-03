/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * mem_catalog.h
 *
 * Author: chenjing
 * Date: 2020/3/25
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_VM_MEM_CATALOG_H_
#define SRC_VM_MEM_CATALOG_H_

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/slice.h"
#include "glog/logging.h"
#include "codec/window.h"
#include "vm/catalog.h"

namespace fesql {
namespace vm {

using fesql::base::Slice;

struct AscComparor {
    bool operator()(std::pair<uint64_t, Slice> i,
                    std::pair<uint64_t, Slice> j) {
        return i.first < j.first;
    }
};

struct DescComparor {
    bool operator()(std::pair<uint64_t, Slice> i,
                    std::pair<uint64_t, Slice> j) {
        return i.first > j.first;
    }
};

typedef std::vector<std::pair<uint64_t, base::Slice>> MemSegment;
typedef std::map<std::string, MemSegment, std::greater<std::string>>
    MemSegmentMap;

class MemTableIterator : public SliceIterator {
 public:
    MemTableIterator(const MemSegment* table, const vm::Schema* schema);
    MemTableIterator(const MemSegment* table, const vm::Schema* schema,
                     int32_t start, int32_t end);
    ~MemTableIterator();
    void Seek(uint64_t ts);
    void SeekToFirst();
    const uint64_t GetKey();
    const base::Slice GetValue();
    void Next();
    bool Valid();

 private:
    const MemSegment* table_;
    const Schema* schema_;
    const MemSegment::const_iterator start_iter_;
    const MemSegment::const_iterator end_iter_;
    MemSegment::const_iterator iter_;
};

class MemWindowIterator : public WindowIterator {
 public:
    MemWindowIterator(const MemSegmentMap* partitions, const Schema* schema);

    ~MemWindowIterator();

    void Seek(const std::string& key);
    void SeekToFirst();
    void Next();
    bool Valid();
    std::unique_ptr<SliceIterator> GetValue();
    const base::Slice GetKey();

 private:
    const MemSegmentMap* partitions_;
    const Schema* schema_;
    MemSegmentMap::const_iterator iter_;
};

class MemRowHandler : public RowHandler {
 public:
    MemRowHandler(base::Slice slice, const Schema* schema)
        : RowHandler(),
          slice_(slice),
          table_name_(""),
          db_(""),
          schema_(schema) {}
    ~MemRowHandler() {}
    const base::Slice GetValue() const { return slice_; }
    const Schema* GetSchema() override { return schema_; }
    const std::string& GetName() override { return table_name_; }
    const std::string& GetDatabase() override { return db_; }

 private:
    const base::Slice slice_;
    std::string table_name_;
    std::string db_;
    const Schema* schema_;
};

class MemTableHandler : public TableHandler {
 public:
    MemTableHandler();
    explicit MemTableHandler(const Schema* schema);
    MemTableHandler(const std::string& table_name, const std::string& db,
                    const Schema* schema);
    const Types& GetTypes() override;
    ~MemTableHandler() override;
    inline const Schema* GetSchema() { return schema_; }
    inline const std::string& GetName() { return table_name_; }
    inline const IndexHint& GetIndex() { return index_hint_; }
    std::unique_ptr<IteratorV<uint64_t, base::Slice>> GetIterator() const;
    IteratorV<uint64_t, base::Slice>* GetIterator(int8_t* addr) const;
    inline const std::string& GetDatabase() { return db_; }
    std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name);
    void AddRow(const base::Slice& v);
    void AddRow(const uint64_t key, const base::Slice& v);
    void Sort(const bool is_asc);
    virtual const uint64_t GetCount() { return table_.size(); }
    virtual Slice At(uint64_t pos) {
        return pos >= 0 && pos < table_.size() ? table_.at(pos).second
                                               : base::Slice();
    }

 protected:
    const std::string table_name_;
    const std::string db_;
    const Schema* schema_;
    Types types_;
    IndexHint index_hint_;
    MemSegment table_;
};

class Window : public MemTableHandler {
 public:
    Window(int64_t start_offset, int64_t end_offset)
        : MemTableHandler(),
          start_(0),
          end_(0),
          start_offset_(start_offset),
          end_offset_(end_offset),
          max_size_(0) {}
    Window(int64_t start_offset, int64_t end_offset, uint32_t max_size)
        : MemTableHandler(),
          start_(0),
          end_(0),
          start_offset_(start_offset),
          end_offset_(end_offset),
          max_size_(max_size) {}
    virtual ~Window() {}

    std::unique_ptr<vm::IteratorV<uint64_t, Slice>> GetIterator()
        const override {
        std::unique_ptr<vm::MemTableIterator> it(
            new vm::MemTableIterator(&table_, schema_, start_, end_));
        return std::move(it);
    }

    vm::IteratorV<uint64_t, Slice>* GetIterator(int8_t* addr) const override {
        if (nullptr == addr) {
            return new vm::MemTableIterator(&table_, schema_, start_, end_);
        } else {
            return new (addr)
                vm::MemTableIterator(&table_, schema_, start_, end_);
        }
    }
    virtual void BufferData(uint64_t key, const Slice& row) = 0;

    virtual const uint64_t GetCount() { return end_ - start_; }
    virtual Slice At(uint64_t pos) {
        return (pos + start_ < end_) ? table_.at(pos + start_).second
                                     : base::Slice();
    }

 protected:
    uint32_t start_;
    uint32_t end_;
    int64_t start_offset_;
    int32_t end_offset_;
    uint32_t max_size_;
};

class CurrentHistoryWindow : public Window {
 public:
    explicit CurrentHistoryWindow(int64_t start_offset)
        : Window(start_offset, 0) {}
    CurrentHistoryWindow(int64_t start_offset, uint32_t max_size)
        : Window(start_offset, 0, max_size) {}

    void BufferData(uint64_t key, const Slice& row) {
        AddRow(key, row);
        end_++;
        int64_t sub = (key + start_offset_);
        uint64_t start_ts = sub < 0 ? 0u : static_cast<uint64_t>(sub);
        while (start_ < end_ &&
               ((0 != max_size_ && (end_ - start_) > max_size_) ||
                (start_ts > 0 && table_[start_].first <= start_ts))) {
            start_++;
        }
    }
};

class CurrentHistoryUnboundWindow : public Window {
 public:
    CurrentHistoryUnboundWindow() : Window(INT64_MIN, 0) {}
    explicit CurrentHistoryUnboundWindow(uint32_t max_size)
        : Window(INT64_MIN, 0, max_size) {}
    void BufferData(uint64_t key, const Slice& row) {
        AddRow(key, row);
        end_++;
        while ((0 != max_size_ && (end_ - start_) > max_size_)) {
            start_++;
        }
    }
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<MemTableHandler>>>
    MemTables;
typedef std::map<std::string, std::shared_ptr<type::Database>> Databases;

class MemPartitionHandler : public PartitionHandler {
 public:
    explicit MemPartitionHandler(const Schema* schema);
    MemPartitionHandler(const std::string& table_name, const std::string& db,
                        const Schema* schema);

    ~MemPartitionHandler();
    const bool IsAsc() override;
    const Types& GetTypes() override;
    const IndexHint& GetIndex() override;
    const Schema* GetSchema() override;
    const std::string& GetName() override;
    const std::string& GetDatabase() override;
    virtual std::unique_ptr<WindowIterator> GetWindowIterator();
    bool AddRow(const std::string& key, uint64_t ts, const Slice& row);
    void Sort(const bool is_asc);
    void Print();
    virtual const uint64_t GetCount() { return partitions_.size(); }

 private:
    std::string table_name_;
    std::string db_;
    const Schema* schema_;
    MemSegmentMap partitions_;
    Types types_;
    IndexHint index_hint_;
    bool is_asc_;
};

class MemCatalog : public Catalog {
 public:
    MemCatalog();

    ~MemCatalog();

    bool Init();

    std::shared_ptr<type::Database> GetDatabase(const std::string& db) {
        return dbs_[db];
    }
    std::shared_ptr<TableHandler> GetTable(const std::string& db,
                                           const std::string& table_name) {
        return tables_[db][table_name];
    }

 private:
    MemTables tables_;
    Databases dbs_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_MEM_CATALOG_H_
