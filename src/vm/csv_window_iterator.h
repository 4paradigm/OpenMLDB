/*
 * csv_window_iterator.h
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

#ifndef SRC_VM_CSV_WINDOW_ITERATOR_H_
#define SRC_VM_CSV_WINDOW_ITERATOR_H_

#include <memory>
#include <string>
#include "arrow/table.h"
#include "vm/catalog.h"
#include "vm/csv_catalog.h"

namespace fesql {
namespace vm {

class CSVWindowIterator : public WindowIterator {
 public:
    CSVWindowIterator(const std::shared_ptr<arrow::Table>& table,
                      const std::string& index_name,
                      const IndexDatas* index_datas, const Schema& schema);

    ~CSVWindowIterator();

    void Seek(const std::string& key);
    void SeekToFirst();
    void Next();
    bool Valid();
    std::unique_ptr<Iterator> GetValue();
    const base::Slice GetKey();

 private:
    const std::shared_ptr<arrow::Table> table_;
    const std::string index_name_;
    const IndexDatas* index_datas_;
    FirstKeyIterator first_it_;
    const Schema schema_;
};

}  // namespace vm
}  // namespace fesql

#endif  // SRC_VM_CSV_WINDOW_ITERATOR_H_
