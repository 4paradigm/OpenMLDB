/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * mem_table_iterator.h
 *
 * Author: chenjing
 * Date: 2020/3/25
 *--------------------------------------------------------------------------
 **/

#include "storage/window.h"
#include "vm/catalog.h"
#ifndef SRC_VM_MEM_TABLE_ITERATOR_H_
#define SRC_VM_MEM_TABLE_ITERATOR_H_
namespace fesql {
namespace vm {

using fesql::storage::Row;
typedef std::vector<std::pair<uint64_t, Row>> MemSegment;
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
    MemWindowIterator(const std::map<std::string, MemSegment>* partitions,
                      const Schema& schema);

    ~MemWindowIterator();

    void Seek(const std::string& key);
    void SeekToFirst();
    void Next();
    bool Valid();
    std::unique_ptr<Iterator> GetValue();
    const base::Slice GetKey();

 private:
    const std::map<std::string, MemSegment>* partitions_;
    const Schema schema_;
    std::map<std::string, MemSegment>::const_iterator iter_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_MEM_TABLE_ITERATOR_H_
