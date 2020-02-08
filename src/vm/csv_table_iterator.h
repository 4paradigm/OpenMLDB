/*
 * csv_table_iterator.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_VM_CSV_TABLE_ITERATOR_H_
#define SRC_VM_CSV_TABLE_ITERATOR_H_

#include "vm/catalog.h"
#include "vm/csv_catalog.h"
#include "storage/codec.h"
#include "arrow/table.h"

namespace fesql {
namespace vm {

uint32_t GetRowSize(const Schema& schema,
                    uint64_t chunk_offset, uint64_t array_offset, 
                    const std::shared_ptr<arrow::Table>& table,
                    storage::RowBuilder* rb);

bool GetRow(const Schema& schema, const std::shared_ptr<arrow::Table>& table,
        uint64_t chunk_offset, uint64_t array_offset,
        storage::RowBuilder* rb);

class CSVSegmentIterator : public Iterator {

 public:
    CSVSegmentIterator(const std::shared_ptr<arrow::Table>& table,
            const std::map<uint64_t, RowLocation>* locations,
            const Schema& schema);
    ~CSVSegmentIterator();

    void Seek(uint64_t ts);

    void SeekToFirst();

    const uint64_t GetKey();

    const base::Slice GetValue();

    void Next();

    bool Valid();

 private:
    const std::shared_ptr<arrow::Table> table_;
    const std::map<uint64_t, RowLocation>* locations_;
    const Schema& schema_;
    int8_t* buf_;
    storage::RowBuilder rb_;
    uint32_t buf_size_;
    std::map<uint64_t, RowLocation>::const_reverse_iterator it_;
};

class CSVTableIterator : public Iterator {

 public:
    CSVTableIterator(const std::shared_ptr<arrow::Table>& table,
            const Schema& schema);
    ~CSVTableIterator();

    void Seek(uint64_t ts);

    void SeekToFirst();

    const uint64_t GetKey();

    const base::Slice GetValue();

    void Next();

    bool Valid();

 private:
    void BuildRow();
 private:
    const std::shared_ptr<arrow::Table> table_;
    const Schema schema_;
    uint64_t chunk_offset_;
    uint64_t array_offset_;
    int8_t* buf_;
    storage::RowBuilder rb_;
    uint32_t buf_size_;
};

}  // namespace vm
}  // namespace fesql

#endif  // SRC_VM_CSV_TABLE_ITERATOR_H_
