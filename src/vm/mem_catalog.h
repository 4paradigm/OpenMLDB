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

#include <map>
#include <memory>
#include <string>
#include "base/slice.h"
#include "glog/logging.h"
#include "storage/codec.h"
#include "storage/window.h"
#include "vm/catalog.h"

namespace fesql {
namespace vm {

using fesql::storage::Row;
typedef std::vector<std::pair<uint64_t, Row>> MemSegment;
struct Comparor {
    bool operator() (std::pair<uint64_t, Row> i, std::pair<uint64_t, Row> j) {
        return i.first > j.first;
    }
};
class MemTableHandler : public TableHandler {
 public:
    MemTableHandler(const Schema& schema);
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
    void Sort();

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
    MemPartitionHandler(const std::shared_ptr<TableHandler>& table_hander,
                        const std::string& index_name)
        : PartitionHandler(table_hander, index_name) {}

    ~MemPartitionHandler() {}

    std::unique_ptr<WindowIterator> GetWindowIterator();
    bool AddRow(const std::string& key, const Row& row);
    bool AddRow(const std::string& key, uint64_t ts, const Row& row);
    void Sort();
    std::map<std::string, MemSegment> partitions_;
};

class MemCatalog : public Catalog {
 public:
    explicit MemCatalog();

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
