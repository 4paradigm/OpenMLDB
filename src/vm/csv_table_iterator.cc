/*
 * csv_table_iterator.cc
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

#include "vm/csv_table_iterator.h"

#include <sstream>  // std::stringstream
#include "arrow/array.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "glog/logging.h"

namespace fesql {
namespace vm {

uint32_t GetRowSize(const Schema& schema, uint64_t chunk_offset,
                    uint64_t array_offset,
                    const std::shared_ptr<arrow::Table>& table,
                    storage::RowBuilder* rb) {
    uint32_t str_size = 0;
    for (int32_t i = 0; i < schema.size(); i++) {
        const type::ColumnDef& column = schema.Get(i);
        if (column.type() == type::kVarchar) {
            auto chunked_array = table->column(i);
            auto array = std::static_pointer_cast<arrow::StringArray>(
                chunked_array->chunk(chunk_offset));
            str_size += array->GetView(array_offset).size();
        }
    }
    uint32_t row_size = rb->CalTotalLength(str_size);
    DLOG(INFO) << "str size " << str_size << " row size " << row_size;
    return row_size;
}

bool GetRow(const Schema& schema, const std::shared_ptr<arrow::Table>& table,
            uint64_t chunk_offset, uint64_t array_offset,
            storage::RowBuilder* rb) {
    std::stringstream ss;
    for (int32_t i = 0; i < schema.size(); i++) {
        const type::ColumnDef& column = schema.Get(i);
        auto chunked_array = table->column(i);
        switch (column.type()) {
            case type::kInt16: {
                auto array = std::static_pointer_cast<arrow::Int16Array>(
                    chunked_array->chunk(chunk_offset));
                int16_t value = array->Value(array_offset);
                ss << value << "\t";
                rb->AppendInt16(value);
                break;
            }
            case type::kInt32: {
                auto array = std::static_pointer_cast<arrow::Int32Array>(
                    chunked_array->chunk(chunk_offset));
                int32_t value = array->Value(array_offset);
                rb->AppendInt32(value);
                ss << value << "\t";
                break;
            }
            case type::kInt64: {
                auto array = std::static_pointer_cast<arrow::Int64Array>(
                    chunked_array->chunk(chunk_offset));
                int64_t value = array->Value(array_offset);
                rb->AppendInt64(value);
                ss << value << "\t";
                break;
            }
            case type::kFloat: {
                auto array = std::static_pointer_cast<arrow::FloatArray>(
                    chunked_array->chunk(chunk_offset));
                float value = array->Value(array_offset);
                rb->AppendFloat(value);
                ss << value << "\t";
                break;
            }
            case type::kDouble: {
                auto array = std::static_pointer_cast<arrow::DoubleArray>(
                    chunked_array->chunk(chunk_offset));
                double value = array->Value(array_offset);
                rb->AppendDouble(value);
                ss << value << "\t";
                break;
            }

            case type::kVarchar: {
                auto array = std::static_pointer_cast<arrow::StringArray>(
                    chunked_array->chunk(chunk_offset));
                auto string_view = array->GetView(array_offset);
                rb->AppendString(string_view.data(), string_view.size());
                std::string value(string_view.data(), string_view.size());
                ss << value << "\t";
                break;
            }
            default: {
                LOG(WARNING) << "type is not supported";
            }
        }
    }
    DLOG(INFO) << ss.str();
    return true;
}

CSVSegmentIterator::CSVSegmentIterator(
    const std::shared_ptr<arrow::Table>& table, const IndexDatas* index_datas,
    const std::string& index_name, const std::string& pk, const Schema& schema)
    : table_(table),
      index_datas_(index_datas),
      index_name_(index_name),
      pk_(pk),
      schema_(schema),
      buf_(NULL),
      rb_(schema),
      buf_size_(0),
      it_() {
    it_ = index_datas_->at(index_name_).at(pk).rbegin();
    rend_ = index_datas_->at(index_name_).at(pk).rend();
}

CSVSegmentIterator::~CSVSegmentIterator() {
    // delete buf_;
}

void CSVSegmentIterator::Seek(uint64_t ts) {
    for (; it_ != rend_; ++it_) {
        if (it_->first <= ts) {
            return;
        }
    }
    it_ = rend_;
}

void CSVSegmentIterator::SeekToFirst() {}

bool CSVSegmentIterator::Valid() {
    bool valid = it_ != rend_;
    if (valid) {
        DLOG(INFO) << "key " << it_->first;
        // TODO(wangtaize) memory leak
        //    if (buf_ != NULL) delete buf_;
        buf_size_ = GetRowSize(schema_, it_->second.chunk_offset,
                               it_->second.array_offset, table_, &rb_);
        buf_ = reinterpret_cast<int8_t*>(malloc(buf_size_));
        rb_.SetBuffer(buf_, buf_size_);
        GetRow(schema_, table_, it_->second.chunk_offset,
               it_->second.array_offset, &rb_);
    }
}

void CSVSegmentIterator::Next() { ++it_; }

const uint64_t CSVSegmentIterator::GetKey() { return it_->first; }

const base::Slice CSVSegmentIterator::GetValue() {
    return base::Slice(reinterpret_cast<char*>(buf_), buf_size_);
}

CSVTableIterator::CSVTableIterator(const std::shared_ptr<arrow::Table>& table,
                                   const Schema& schema)
    : table_(table),
      schema_(schema),
      chunk_offset_(0),
      array_offset_(0),
      buf_(NULL),
      rb_(schema),
      buf_size_(0) {}

CSVTableIterator::~CSVTableIterator() {
    // delete buf_;
}

void CSVTableIterator::Seek(uint64_t ts) {}

void CSVTableIterator::SeekToFirst() {}

const uint64_t CSVTableIterator::GetKey() { return 0; }

const base::Slice CSVTableIterator::GetValue() {
    return base::Slice(reinterpret_cast<char*>(buf_), buf_size_);
}

void CSVTableIterator::Next() {
    if (table_->column(0)->chunk(chunk_offset_)->length() <=
        array_offset_ + 1) {
        chunk_offset_ += 1;
        array_offset_ = 0;
    } else {
        array_offset_ += 1;
    }
}

bool CSVTableIterator::Valid() {
    if (table_->num_columns() <= 0) return false;
    if (table_->column(0)->num_chunks() <= chunk_offset_) return false;
    if (table_->column(0)->chunk(chunk_offset_)->length() <= array_offset_)
        return false;
    BuildRow();
    return true;
}

void CSVTableIterator::BuildRow() {
    uint32_t row_size =
        GetRowSize(schema_, chunk_offset_, array_offset_, table_, &rb_);
    // if (buf_ != NULL) delete buf_;
    buf_ = reinterpret_cast<int8_t*>(malloc(row_size));
    rb_.SetBuffer(buf_, row_size);
    buf_size_ = row_size;
    GetRow(schema_, table_, chunk_offset_, array_offset_, &rb_);
}

}  // namespace vm
}  // namespace fesql
