/*
 * csv_window_iterator.cc
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

#include "vm/csv_window_iterator.h"

#include "vm/csv_table_iterator.h"

namespace fesql {
namespace vm {

CSVWindowIterator::CSVWindowIterator(const std::shared_ptr<arrow::Table>& table,
        const IndexDatas* index_datas, const std::string& key, const Schema& schema):table_(table),
    index_datas_(index_datas), index_name_(key), index_data_(index_datas->at(key)) ,
    schema_(schema){
    first_it_ = index_data_.begin();
}

CSVWindowIterator::~CSVWindowIterator() {}


void CSVWindowIterator::Seek(const std::string& key) {
    first_it_ = index_data_.find(key);
}

void CSVWindowIterator::SeekToFirst() {}

bool CSVWindowIterator::Valid() {
    return first_it_ != index_data_.end();
}

void CSVWindowIterator::Next() {
    ++first_it_;
}

const base::Slice CSVWindowIterator::GetKey() {
    return base::Slice(first_it_->first);
}

std::unique_ptr<Iterator> CSVWindowIterator::GetValue() {
    auto second = first_it_->second;
    std::unique_ptr<CSVSegmentIterator> segment_it(new CSVSegmentIterator(table_, &second,
                schema_));
    return std::move(segment_it);
}

}  // namespace vm
}  // namespace fesql


