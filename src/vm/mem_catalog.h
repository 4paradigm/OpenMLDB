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
#include "storage/codec.h"
#include "storage/window.h"
#include "vm/catalog.h"

namespace fesql {
namespace vm {

using fesql::storage::Row;

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

typedef std::vector<std::pair<uint64_t, Row>> MemSegment;
typedef std::vector<std::pair<uint64_t, Row>> MemSegment;
typedef std::map<std::string, MemSegment, std::greater<std::string>>
    MemSegmentMap;

class MemTableIterator : public Iterator {
 public:
    MemTableIterator(const MemSegment* table, const vm::Schema& schema);
    ~MemTableIterator();
    void Seek(uint64_t ts);

    void SeekToFirst();

    const uint64_t GetKey();

    const base::Slice GetValue();

    void Next();

    bool Valid();

 private:
    const MemSegment* table_;
    const Schema& schema_;
    MemSegment::const_iterator iter_;
};

class MemWindowIterator : public WindowIterator {
 public:
    MemWindowIterator(const MemSegmentMap* partitions, const Schema& schema);

    ~MemWindowIterator();

    void Seek(const std::string& key);
    void SeekToFirst();
    void Next();
    bool Valid();
    std::unique_ptr<Iterator> GetValue();
    const base::Slice GetKey();

 private:
    const MemSegmentMap* partitions_;
    const Schema schema_;
    MemSegmentMap::const_iterator iter_;
};

class MemRowHandler : public RowHandler {
 public:
    MemRowHandler(base::Slice slice, const Schema& schema)
        : RowHandler(), slice_(slice), table_name_(""), db_(""), schema_(schema) {}
//    MemRowHandler(base::Slice& slice, const std::string& table_name,
//                  const std::string& db, const Schema& schema)
//        : RowHandler(),
//          slice_(slice),
//          table_name_(table_name),
//          db_(db),
//          schema_(schema) {}
    ~MemRowHandler() {}
    const base::Slice GetValue() const { return slice_; }
    const Schema& GetSchema() override { return schema_; }
    const std::string& GetName() override { return table_name_; }
    const std::string& GetDatabase() override { return db_; }

 private:
    const base::Slice slice_;
    std::string table_name_;
    std::string db_;
    const Schema& schema_;
};
class MemTableHandler : public TableHandler {
 public:
    explicit MemTableHandler(const Schema& schema);
    MemTableHandler(const std::string& table_name, const std::string& db,
                    const Schema& schema);
    const Types& GetTypes() override;
    ~MemTableHandler() override;
    inline const Schema& GetSchema() { return schema_; }
    inline const std::string& GetName() { return table_name_; }
    inline const IndexHint& GetIndex() { return index_hint_; }
    std::unique_ptr<Iterator> GetIterator();
    inline const std::string& GetDatabase() { return db_; }
    std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name);
    void AddRow(const Row& row);
    void AddRow(const uint64_t key, const Row& row);
    void Sort(const bool is_asc);
    const base::Slice Get(int32_t pos) override;

 private:
    std::string table_name_;
    std::string db_;
    const Schema& schema_;
    Types types_;
    MemSegment table_;
    IndexHint index_hint_;
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<MemTableHandler>>>
    MemTables;

typedef std::map<std::string, std::shared_ptr<type::Database>> Databases;

class MemPartitionHandler : public PartitionHandler {
 public:
    explicit MemPartitionHandler(const Schema& schema);
    MemPartitionHandler(const std::string& table_name, const std::string& db,
                        const Schema& schema);

    ~MemPartitionHandler();
    const bool IsAsc() override;
    const Types& GetTypes() override;
    const IndexHint& GetIndex() override;
    const Schema& GetSchema() override;
    const std::string& GetName() override;
    const std::string& GetDatabase() override;
    virtual std::unique_ptr<WindowIterator> GetWindowIterator();
    bool AddRow(const std::string& key, uint64_t ts, const Row& row);
    void Sort(const bool is_asc);
    void Print();

 private:
    std::string table_name_;
    std::string db_;
    const Schema& schema_;
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
