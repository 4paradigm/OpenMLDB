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
#include "codec/list_iterator_codec.h"
#include "glog/logging.h"
#include "vm/catalog.h"

namespace fesql {
namespace vm {

using fesql::codec::IteratorV;
using fesql::codec::Row;
using fesql::codec::RowIterator;
using fesql::codec::WindowIterator;

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

typedef std::vector<std::pair<uint64_t, Row>> MemTimeTable;
typedef std::vector<Row> MemTable;
typedef std::map<std::string, MemTimeTable, std::greater<std::string>>
    MemSegmentMap;

class MemTimeTableIterator : public RowIterator {
 public:
    MemTimeTableIterator(const MemTimeTable* table, const vm::Schema* schema);
    MemTimeTableIterator(const MemTimeTable* table, const vm::Schema* schema,
                         int32_t start, int32_t end);
    ~MemTimeTableIterator();
    void Seek(uint64_t ts);
    void SeekToFirst();
    const uint64_t GetKey();
    const Row& GetValue();
    void Next();
    bool Valid();

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
    void Seek(uint64_t ts);
    void SeekToFirst();
    const uint64_t GetKey();
    const Row& GetValue();
    void Next();
    bool Valid();

 private:
    const MemTable* table_;
    const Schema* schema_;
    const MemTable::const_iterator start_iter_;
    const MemTable::const_iterator end_iter_;
    MemTable::const_iterator iter_;
};

class MemWindowIterator : public WindowIterator {
 public:
    MemWindowIterator(const MemSegmentMap* partitions, const Schema* schema);

    ~MemWindowIterator();

    void Seek(const std::string& key);
    void SeekToFirst();
    void Next();
    bool Valid();
    std::unique_ptr<RowIterator> GetValue();
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
    const Row& GetValue() const override { return row_; }
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
    inline const Schema* GetSchema() { return schema_; }
    inline const std::string& GetName() { return table_name_; }
    inline const IndexHint& GetIndex() { return index_hint_; }
    inline const std::string& GetDatabase() { return db_; }

    std::unique_ptr<IteratorV<uint64_t, Row>> GetIterator() const;
    IteratorV<uint64_t, Row>* GetIterator(int8_t* addr) const;
    std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name);

    void AddRow(const Row& row);
    void Reverse();
    virtual const uint64_t GetCount() { return table_.size(); }
    virtual Row At(uint64_t pos) {
        return pos >= 0 && pos < table_.size() ? table_.at(pos) : Row();
    }

    const std::string GetHandlerTypeName() override {
        return "MemTableHandler";
    }

 protected:
    const std::string table_name_;
    const std::string db_;
    const Schema* schema_;
    Types types_;
    IndexHint index_hint_;
    MemTable table_;
};

class MemTimeTableHandler : public TableHandler {
 public:
    MemTimeTableHandler();
    explicit MemTimeTableHandler(const Schema* schema);
    MemTimeTableHandler(const std::string& table_name, const std::string& db,
                        const Schema* schema);
    const Types& GetTypes() override;
    ~MemTimeTableHandler() override;
    inline const Schema* GetSchema() { return schema_; }
    inline const std::string& GetName() { return table_name_; }
    inline const IndexHint& GetIndex() { return index_hint_; }
    std::unique_ptr<IteratorV<uint64_t, Row>> GetIterator() const;
    IteratorV<uint64_t, Row>* GetIterator(int8_t* addr) const;
    inline const std::string& GetDatabase() { return db_; }
    std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name);
    void AddRow(const uint64_t key, const Row& v);
    void AddRow(const Row& v);
    void Sort(const bool is_asc);
    void Reverse();
    virtual const uint64_t GetCount() { return table_.size(); }
    virtual Row At(uint64_t pos) {
        return pos >= 0 && pos < table_.size() ? table_.at(pos).second : Row();
    }
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
};

class Window : public MemTimeTableHandler {
 public:
    Window(int64_t start_offset, int64_t end_offset)
        : MemTimeTableHandler(),
          start_(0),
          end_(0),
          start_offset_(start_offset),
          end_offset_(end_offset),
          max_size_(0) {}
    Window(int64_t start_offset, int64_t end_offset, uint32_t max_size)
        : MemTimeTableHandler(),
          start_(0),
          end_(0),
          start_offset_(start_offset),
          end_offset_(end_offset),
          max_size_(max_size) {}
    virtual ~Window() {}

    std::unique_ptr<vm::IteratorV<uint64_t, Row>> GetIterator() const override {
        std::unique_ptr<vm::MemTimeTableIterator> it(
            new vm::MemTimeTableIterator(&table_, schema_, start_, end_));
        return std::move(it);
    }

    vm::IteratorV<uint64_t, Row>* GetIterator(int8_t* addr) const override {
        if (nullptr == addr) {
            return new vm::MemTimeTableIterator(&table_, schema_, start_, end_);
        } else {
            return new (addr)
                vm::MemTimeTableIterator(&table_, schema_, start_, end_);
        }
    }
    virtual void BufferData(uint64_t key, const Row& row) = 0;

    virtual const uint64_t GetCount() { return end_ - start_; }
    virtual Row At(uint64_t pos) {
        return (pos + start_ < end_) ? table_.at(pos + start_).second : Row();
    }
    const std::string GetHandlerTypeName() override { return "Window"; }

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

    void BufferData(uint64_t key, const Row& row) {
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
    void BufferData(uint64_t key, const Row& row) {
        AddRow(key, row);
        end_++;
        while ((0 != max_size_ && (end_ - start_) > max_size_)) {
            start_++;
        }
    }
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

    inline const vm::Schema* GetSchema() {
        return partition_hander_->GetSchema();
    }

    inline const std::string& GetName() { return partition_hander_->GetName(); }

    inline const std::string& GetDatabase() {
        return partition_hander_->GetDatabase();
    }

    inline const vm::Types& GetTypes() { return partition_hander_->GetTypes(); }

    inline const vm::IndexHint& GetIndex() {
        return partition_hander_->GetIndex();
    }

    std::unique_ptr<vm::RowIterator> GetIterator() const {
        auto iter = partition_hander_->GetWindowIterator();
        if (iter) {
            iter->Seek(key_);
            return iter->Valid() ? std::move(iter->GetValue())
                                 : std::unique_ptr<RowIterator>();
        }
        return std::unique_ptr<RowIterator>();
    }
    vm::IteratorV<uint64_t, Row>* GetIterator(int8_t* addr) const override {
        LOG(WARNING) << "can't get iterator with given address";
        return nullptr;
    }
    std::unique_ptr<vm::WindowIterator> GetWindowIterator(
        const std::string& idx_name) {
        LOG(WARNING) << "SegmentHandler can't support window iterator";
        return std::unique_ptr<WindowIterator>();
    }
    virtual const uint64_t GetCount() {
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
        if (pos < 0) {
            return Row();
        }
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

class MemPartitionHandler : public PartitionHandler {
 public:
    MemPartitionHandler() {}
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
    bool AddRow(const std::string& key, uint64_t ts, const Row& row);
    void Sort(const bool is_asc);
    void Reverse();
    void Print();
    virtual const uint64_t GetCount() { return partitions_.size(); }
    virtual std::shared_ptr<TableHandler> GetSegment(
        std::shared_ptr<PartitionHandler> partition_hander,
        const std::string& key) {
        return std::shared_ptr<MemSegmentHandler>(
            new MemSegmentHandler(partition_hander, key));
    }

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
