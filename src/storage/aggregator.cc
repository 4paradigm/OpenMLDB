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

#include "boost/algorithm/string.hpp"

#include "base/glog_wapper.h"
#include "base/slice.h"
#include "base/strings.h"
#include "common/timer.h"
#include "storage/aggregator.h"
#include "storage/table.h"

namespace openmldb {
namespace storage {

using ::openmldb::base::StringCompare;
Aggregator::Aggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                       std::shared_ptr<Table> aggr_table, const uint32_t& index_pos, const std::string& aggr_col,
                       const AggrType& aggr_type, const std::string& ts_col, WindowType window_tpye,
                       uint32_t window_size)
    : base_table_schema_(base_meta.column_desc()),
      aggr_table_schema_(aggr_meta.column_desc()),
      aggr_table_(aggr_table),
      index_pos_(index_pos),
      aggr_col_(aggr_col),
      aggr_type_(aggr_type),
      ts_col_(ts_col),
      window_type_(window_tpye),
      window_size_(window_size),
      base_row_view_(base_table_schema_),
      aggr_row_view_(aggr_table_schema_),
      row_builder_(aggr_table_schema_) {
    for (int i = 0; i < base_meta.column_desc().size(); i++) {
        if (base_meta.column_desc(i).name() == aggr_col_) {
            aggr_col_idx_ = i;
        }
        if (base_meta.column_desc(i).name() == ts_col_) {
            ts_col_idx_ = i;
        }
    }
    aggr_col_type_ = base_meta.column_desc(aggr_col_idx_).data_type();
    ts_col_type_ = base_meta.column_desc(ts_col_idx_).data_type();
    auto dimension = dimensions_.Add();
    dimension->set_idx(0);
}

Aggregator::~Aggregator() {
    if (aggr_col_type_ == DataType::kString || aggr_col_type_ == DataType::kVarchar) {
        for (const auto& it : aggr_buffer_map_) {
            if (it.second.buffer_.aggr_val_.vstring.data)
                delete[] it.second.buffer_.aggr_val_.vstring.data;
        }
    }
}

bool Aggregator::Update(const std::string& key, const std::string& row, const uint64_t& offset) {
    int8_t* row_ptr = reinterpret_cast<int8_t*>(const_cast<char*>(row.c_str()));
    int64_t cur_ts;
    switch (ts_col_type_) {
        case DataType::kBigInt: {
            base_row_view_.GetValue(row_ptr, ts_col_idx_, DataType::kBigInt, &cur_ts);
            break;
        }
        case DataType::kTimestamp: {
            base_row_view_.GetValue(row_ptr, ts_col_idx_, DataType::kTimestamp, &cur_ts);
            break;
        }
        default: {
            PDLOG(ERROR, "Unsupported timestamp data type");
            return false;
        }
    }

    AggrBufferLocked* aggr_buffer_lock;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = aggr_buffer_map_.find(key);
        if (it == aggr_buffer_map_.end()) {
            auto insert_pair = aggr_buffer_map_.emplace(key, AggrBufferLocked{});
            aggr_buffer_lock = &insert_pair.first->second;
        } else {
            aggr_buffer_lock = &it->second;
        }
    }

    std::unique_lock<std::mutex> lock(*aggr_buffer_lock->mu_);
    AggrBuffer& aggr_buffer = aggr_buffer_lock->buffer_;

    // init buffer timestamp range
    if (aggr_buffer.ts_begin_ == -1) {
        aggr_buffer.ts_begin_ = cur_ts;
        if (window_type_ == WindowType::kRowsRange) {
            aggr_buffer.ts_end_ = cur_ts + window_size_ - 1;
        }
    }

    if (CheckBufferFilled(cur_ts, aggr_buffer.ts_end_, aggr_buffer.aggr_cnt_)) {
        AggrBuffer flush_buffer = aggr_buffer;
        int64_t latest_ts = aggr_buffer.ts_end_ + 1;
        aggr_buffer.clear();
        aggr_buffer.ts_begin_ = latest_ts;
        if (window_type_ == WindowType::kRowsRange) {
            aggr_buffer.ts_end_ = latest_ts + window_size_ - 1;
        }
        lock.unlock();
        FlushAggrBuffer(key, flush_buffer);
        lock.lock();
    }

    if (cur_ts < aggr_buffer.ts_begin_) {
        // handle the case that the current timestamp is smaller than the begin timestamp in aggregate buffer
        lock.unlock();
        bool ok = UpdateFlushedBuffer(key, row_ptr, cur_ts, offset);
        if (!ok) {
            PDLOG(ERROR, "Update flushed buffer failed");
            return false;
        }
    } else {
        aggr_buffer.aggr_cnt_++;
        aggr_buffer.binlog_offset_ = offset;
        if (window_type_ == WindowType::kRowsNum) {
            aggr_buffer.ts_end_ = cur_ts;
        }
        bool ok = UpdateAggrVal(base_row_view_, row_ptr, &aggr_buffer);
        if (!ok) {
            PDLOG(ERROR, "Update aggr value failed");
            return false;
        }
    }
    return true;
}

bool Aggregator::GetAggrBuffer(const std::string& key, AggrBuffer* buffer) {
    std::lock_guard<std::mutex> lock(mu_);
    if (aggr_buffer_map_.find(key) == aggr_buffer_map_.end()) {
        return false;
    }
    *buffer = aggr_buffer_map_.at(key).buffer_;
    return true;
}

bool Aggregator::GetAggrBufferFromRowView(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* buffer) {
    if (buffer == nullptr) {
        return false;
    }
    row_view.GetValue(row_ptr, 1, DataType::kTimestamp, &buffer->ts_begin_);
    row_view.GetValue(row_ptr, 2, DataType::kTimestamp, &buffer->ts_end_);
    row_view.GetValue(row_ptr, 3, DataType::kInt, &buffer->aggr_cnt_);
    char* ch = NULL;
    uint32_t ch_length = 0;
    row_view.GetValue(row_ptr, 4, &ch, &ch_length);
    switch (aggr_col_type_) {
        case DataType::kSmallInt:
        case DataType::kInt:
        case DataType::kBigInt: {
            int64_t origin_val = *reinterpret_cast<int64_t*>(ch);
            buffer->aggr_val_.vlong = origin_val;
            break;
        }
        case DataType::kFloat: {
            float origin_val = *reinterpret_cast<float*>(ch);
            buffer->aggr_val_.vfloat = origin_val;
            break;
        }
        case DataType::kDouble: {
            double origin_val = *reinterpret_cast<double*>(ch);
            buffer->aggr_val_.vdouble = origin_val;
            break;
        }
        default: {
            PDLOG(ERROR, "Unsupported data type");
            return false;
        }
    }
    return true;
}

bool Aggregator::FlushAggrBuffer(const std::string& key, const AggrBuffer& buffer) {
    std::string encoded_row;
    std::string aggr_val;
    if (!EncodeAggrVal(buffer, &aggr_val)) {
        PDLOG(ERROR, "Enocde aggr value to row failed");
        return false;
    }
    int str_length = key.size() + aggr_val.size();
    uint32_t row_size = row_builder_.CalTotalLength(str_length);
    encoded_row.resize(row_size);
    int8_t* row_ptr = reinterpret_cast<int8_t*>(&(encoded_row[0]));
    row_builder_.InitBuffer(row_ptr, row_size, true);
    row_builder_.SetString(row_ptr, row_size, 0, key.c_str(), key.size());
    row_builder_.SetTimestamp(row_ptr, 1, buffer.ts_begin_);
    row_builder_.SetTimestamp(row_ptr, 2, buffer.ts_end_);
    if ((aggr_type_ == AggrType::kMax || aggr_type_ == AggrType::kMin) && buffer.AggrValEmpty()) {
        row_builder_.SetNULL(row_ptr, row_size, 4);
    } else {
        row_builder_.SetString(row_ptr, row_size, 4, aggr_val.c_str(), aggr_val.size());
    }
    row_builder_.SetInt32(row_ptr, 3, buffer.aggr_cnt_);
    row_builder_.SetInt64(row_ptr, 5, buffer.binlog_offset_);

    int64_t time = ::baidu::common::timer::get_micros() / 1000;
    dimensions_.Mutable(0)->set_key(key);
    bool ok = aggr_table_->Put(time, encoded_row, dimensions_);
    if (!ok) {
        PDLOG(ERROR, "Aggregator put failed");
        return false;
    }
    return true;
}

bool Aggregator::UpdateFlushedBuffer(const std::string& key, const int8_t* base_row_ptr, int64_t cur_ts,
                                     uint64_t offset) {
    auto it = aggr_table_->NewTraverseIterator(0);
    // If there is no repetition of ts, `seek` will locate to the position that less than ts.
    it->Seek(key, cur_ts + 1);
    AggrBuffer tmp_buffer;
    if (it->Valid()) {
        auto val = it->GetValue();
        int8_t* aggr_row_ptr = reinterpret_cast<int8_t*>(const_cast<char*>(val.data()));

        bool ok = GetAggrBufferFromRowView(aggr_row_view_, aggr_row_ptr, &tmp_buffer);
        if (!ok) {
            PDLOG(ERROR, "GetAggrBufferFromRowView failed");
            return false;
        }
        if (cur_ts > tmp_buffer.ts_end_ || cur_ts < tmp_buffer.ts_begin_) {
            PDLOG(ERROR, "Current ts isn't in buffer range");
            return false;
        }
        tmp_buffer.aggr_cnt_ += 1;
        tmp_buffer.binlog_offset_ = offset;
    } else {
        tmp_buffer.ts_begin_ = cur_ts;
        tmp_buffer.ts_end_ = cur_ts;
        tmp_buffer.aggr_cnt_ = 1;
        tmp_buffer.binlog_offset_ = offset;
    }
    bool ok = UpdateAggrVal(base_row_view_, base_row_ptr, &tmp_buffer);
    if (!ok) {
        PDLOG(ERROR, "UpdateAggrVal failed");
        return false;
    }
    ok = FlushAggrBuffer(key, tmp_buffer);
    if (!ok) {
        PDLOG(ERROR, "FlushAggrBuffer failed");
        return false;
    }
    return true;
}

bool Aggregator::CheckBufferFilled(int64_t cur_ts, int64_t buffer_end, int32_t buffer_cnt) {
    if (window_type_ == WindowType::kRowsRange && cur_ts > buffer_end) {
        return true;
    } else if (window_type_ == WindowType::kRowsNum && buffer_cnt >= window_size_) {
        return true;
    }
    return false;
}

SumAggregator::SumAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                             std::shared_ptr<Table> aggr_table, const uint32_t& index_pos, const std::string& aggr_col,
                             const AggrType& aggr_type, const std::string& ts_col, WindowType window_tpye,
                             uint32_t window_size)
    : Aggregator(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, aggr_type, ts_col, window_tpye, window_size) {}

bool SumAggregator::UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer) {
    if (row_view.IsNULL(row_ptr, aggr_col_idx_)) {
        return true;
    }
    switch (aggr_col_type_) {
        case DataType::kSmallInt: {
            int16_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            aggr_buffer->aggr_val_.vlong += val;
            break;
        }
        case DataType::kInt: {
            int32_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            aggr_buffer->aggr_val_.vlong += val;
            break;
        }
        case DataType::kBigInt: {
            int64_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            aggr_buffer->aggr_val_.vlong += val;
            break;
        }
        case DataType::kFloat: {
            float val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            aggr_buffer->aggr_val_.vfloat += val;
            break;
        }
        case DataType::kDouble: {
            double val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            aggr_buffer->aggr_val_.vdouble += val;
            break;
        }
        default: {
            PDLOG(ERROR, "Unsupported data type");
            return false;
        }
    }
    aggr_buffer->non_null_cnt++;
    return true;
}

bool SumAggregator::EncodeAggrVal(const AggrBuffer& buffer, std::string* aggr_val) {
    switch (aggr_col_type_) {
        case DataType::kSmallInt:
        case DataType::kInt:
        case DataType::kBigInt: {
            int64_t tmp_val = buffer.aggr_val_.vlong;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(int64_t));
            break;
        }
        case DataType::kFloat: {
            float tmp_val = buffer.aggr_val_.vfloat;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(float));
            break;
        }
        case DataType::kDouble: {
            double tmp_val = buffer.aggr_val_.vdouble;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(double));
            break;
        }
        default: {
            PDLOG(ERROR, "Unsupported data type");
            return false;
        }
    }
    return true;
}

MinMaxBaseAggregator::MinMaxBaseAggregator(const ::openmldb::api::TableMeta& base_meta,
                                           const ::openmldb::api::TableMeta& aggr_meta,
                                           std::shared_ptr<Table> aggr_table, const uint32_t& index_pos,
                                           const std::string& aggr_col, const AggrType& aggr_type,
                                           const std::string& ts_col, WindowType window_tpye, uint32_t window_size)
    : Aggregator(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, aggr_type, ts_col, window_tpye, window_size) {}

bool MinMaxBaseAggregator::EncodeAggrVal(const AggrBuffer& buffer, std::string* aggr_val) {
    switch (aggr_col_type_) {
        case DataType::kSmallInt: {
            int16_t tmp_val = buffer.aggr_val_.vsmallint;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(int16_t));
            break;
        }
        case DataType::kDate:
        case DataType::kInt: {
            int32_t tmp_val = buffer.aggr_val_.vint;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(int32_t));
            break;
        }
        case DataType::kTimestamp:
        case DataType::kBigInt: {
            int64_t tmp_val = buffer.aggr_val_.vlong;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(int64_t));
            break;
        }
        case DataType::kFloat: {
            float tmp_val = buffer.aggr_val_.vfloat;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(float));
            break;
        }
        case DataType::kDouble: {
            double tmp_val = buffer.aggr_val_.vdouble;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(double));
            break;
        }
        case DataType::kString:
        case DataType::kVarchar: {
            aggr_val->assign(buffer.aggr_val_.vstring.data, buffer.aggr_val_.vstring.len);
            break;
        }
        default: {
            PDLOG(ERROR, "Unsupported data type");
            return false;
        }
    }
    return true;
}

MinAggregator::MinAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                             std::shared_ptr<Table> aggr_table, const uint32_t& index_pos, const std::string& aggr_col,
                             const AggrType& aggr_type, const std::string& ts_col, WindowType window_tpye,
                             uint32_t window_size)
    : MinMaxBaseAggregator(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, aggr_type, ts_col, window_tpye,
                           window_size) {}

bool MinAggregator::UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer) {
    if (row_view.IsNULL(row_ptr, aggr_col_idx_)) {
        return true;
    }
    switch (aggr_col_type_) {
        case DataType::kSmallInt: {
            int16_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            if (aggr_buffer->AggrValEmpty() || val < aggr_buffer->aggr_val_.vsmallint) {
                aggr_buffer->aggr_val_.vsmallint = val;
            }
            break;
        }
        case DataType::kDate:
        case DataType::kInt: {
            int32_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            if (aggr_buffer->AggrValEmpty() || val < aggr_buffer->aggr_val_.vint) {
                aggr_buffer->aggr_val_.vint = val;
            }
            break;
        }
        case DataType::kTimestamp:
        case DataType::kBigInt: {
            int64_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            if (aggr_buffer->AggrValEmpty() || val < aggr_buffer->aggr_val_.vlong) {
                aggr_buffer->aggr_val_.vlong = val;
            }
            break;
        }
        case DataType::kFloat: {
            float val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            if (aggr_buffer->AggrValEmpty() || val < aggr_buffer->aggr_val_.vfloat) {
                aggr_buffer->aggr_val_.vfloat = val;
            }
            break;
        }
        case DataType::kDouble: {
            double val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            if (aggr_buffer->AggrValEmpty() || val < aggr_buffer->aggr_val_.vdouble) {
                aggr_buffer->aggr_val_.vdouble = val;
            }
            break;
        }
        case DataType::kString:
        case DataType::kVarchar: {
            char* ch = NULL;
            uint32_t ch_length = 0;
            row_view.GetValue(row_ptr, aggr_col_idx_, &ch, &ch_length);
            auto& aggr_val = aggr_buffer->aggr_val_.vstring;
            if (aggr_buffer->AggrValEmpty() || StringCompare(ch, ch_length, aggr_val.data, aggr_val.len) < 0) {
                if (aggr_val.data != NULL && ch_length > aggr_val.len) {
                    delete[] aggr_val.data;
                    aggr_val.data = NULL;
                }
                if (aggr_val.data == NULL) {
                    aggr_val.data = new char[ch_length];
                }
                aggr_val.len = ch_length;
                memcpy(aggr_val.data, ch, ch_length);
            }
            break;
        }
        default: {
            PDLOG(ERROR, "Unsupported data type");
            return false;
        }
    }
    aggr_buffer->non_null_cnt++;
    return true;
}

MaxAggregator::MaxAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                             std::shared_ptr<Table> aggr_table, const uint32_t& index_pos, const std::string& aggr_col,
                             const AggrType& aggr_type, const std::string& ts_col, WindowType window_tpye,
                             uint32_t window_size)
    : MinMaxBaseAggregator(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, aggr_type, ts_col, window_tpye,
                           window_size) {}

bool MaxAggregator::UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer) {
    if (row_view.IsNULL(row_ptr, aggr_col_idx_)) {
        return true;
    }
    switch (aggr_col_type_) {
        case DataType::kSmallInt: {
            int16_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            if (aggr_buffer->AggrValEmpty() || val > aggr_buffer->aggr_val_.vsmallint) {
                aggr_buffer->aggr_val_.vsmallint = val;
            }
            break;
        }
        case DataType::kDate:
        case DataType::kInt: {
            int32_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            if (aggr_buffer->AggrValEmpty() || val > aggr_buffer->aggr_val_.vint) {
                aggr_buffer->aggr_val_.vint = val;
            }
            break;
        }
        case DataType::kTimestamp:
        case DataType::kBigInt: {
            int64_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            if (aggr_buffer->AggrValEmpty() || val > aggr_buffer->aggr_val_.vlong) {
                aggr_buffer->aggr_val_.vlong = val;
            }
            break;
        }
        case DataType::kFloat: {
            float val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            if (aggr_buffer->AggrValEmpty() || val > aggr_buffer->aggr_val_.vfloat) {
                aggr_buffer->aggr_val_.vfloat = val;
            }
            break;
        }
        case DataType::kDouble: {
            double val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            if (aggr_buffer->AggrValEmpty() || val > aggr_buffer->aggr_val_.vdouble) {
                aggr_buffer->aggr_val_.vdouble = val;
            }
            break;
        }
        case DataType::kString:
        case DataType::kVarchar: {
            char* ch = NULL;
            uint32_t ch_length = 0;
            row_view.GetValue(row_ptr, aggr_col_idx_, &ch, &ch_length);
            auto& aggr_val = aggr_buffer->aggr_val_.vstring;
            if (aggr_buffer->AggrValEmpty() || StringCompare(ch, ch_length, aggr_val.data, aggr_val.len) > 0) {
                if (aggr_val.data != NULL && ch_length > aggr_val.len) {
                    delete[] aggr_val.data;
                    aggr_val.data = NULL;
                }
                if (aggr_val.data == NULL) {
                    aggr_val.data = new char[ch_length];
                }
                aggr_val.len = ch_length;
                memcpy(aggr_val.data, ch, ch_length);
            }
            break;
        }
        default: {
            PDLOG(ERROR, "Unsupported data type");
            return false;
        }
    }
    aggr_buffer->non_null_cnt++;
    return true;
}

CountAggregator::CountAggregator(const ::openmldb::api::TableMeta& base_meta,
                                 const ::openmldb::api::TableMeta& aggr_meta, std::shared_ptr<Table> aggr_table,
                                 const uint32_t& index_pos, const std::string& aggr_col, const AggrType& aggr_type,
                                 const std::string& ts_col, WindowType window_tpye, uint32_t window_size)
    : Aggregator(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, aggr_type, ts_col, window_tpye, window_size) {}

bool CountAggregator::EncodeAggrVal(const AggrBuffer& buffer, std::string* aggr_val) {
    int64_t tmp_val = buffer.non_null_cnt;
    aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(int64_t));
    return true;
}

bool CountAggregator::UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer) {
    if (!row_view.IsNULL(row_ptr, aggr_col_idx_)) {
        aggr_buffer->non_null_cnt++;
    }
    return true;
}

AvgAggregator::AvgAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                             std::shared_ptr<Table> aggr_table, const uint32_t& index_pos, const std::string& aggr_col,
                             const AggrType& aggr_type, const std::string& ts_col, WindowType window_tpye,
                             uint32_t window_size)
    : Aggregator(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, aggr_type, ts_col, window_tpye, window_size) {}

bool AvgAggregator::UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer) {
    if (row_view.IsNULL(row_ptr, aggr_col_idx_)) {
        return true;
    }
    switch (aggr_col_type_) {
        case DataType::kSmallInt: {
            int16_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            aggr_buffer->aggr_val_.vlong += val;
            break;
        }
        case DataType::kInt: {
            int32_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            aggr_buffer->aggr_val_.vlong += val;
            break;
        }
        case DataType::kBigInt: {
            int64_t val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            aggr_buffer->aggr_val_.vlong += val;
            break;
        }
        case DataType::kFloat: {
            float val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            aggr_buffer->aggr_val_.vfloat += val;
            break;
        }
        case DataType::kDouble: {
            double val;
            row_view.GetValue(row_ptr, aggr_col_idx_, aggr_col_type_, &val);
            aggr_buffer->aggr_val_.vdouble += val;
            break;
        }
        default: {
            PDLOG(ERROR, "Unsupported data type");
            return false;
        }
    }
    aggr_buffer->non_null_cnt++;
    return true;
}

bool AvgAggregator::EncodeAggrVal(const AggrBuffer& buffer, std::string* aggr_val) {
    switch (aggr_col_type_) {
        case DataType::kSmallInt:
        case DataType::kInt:
        case DataType::kBigInt: {
            int64_t tmp_val = buffer.aggr_val_.vlong;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(int64_t));
            break;
        }
        case DataType::kFloat: {
            float tmp_val = buffer.aggr_val_.vfloat;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(float));
            break;
        }
        case DataType::kDouble: {
            double tmp_val = buffer.aggr_val_.vdouble;
            aggr_val->assign(reinterpret_cast<char*>(&tmp_val), sizeof(double));
            break;
        }
        default: {
            PDLOG(ERROR, "Unsupported data type");
            return false;
        }
    }
    aggr_val->append(reinterpret_cast<char*>(const_cast<int64_t*>(&buffer.non_null_cnt)), sizeof(int64_t));
    return true;
}

std::shared_ptr<Aggregator> CreateAggregator(const ::openmldb::api::TableMeta& base_meta,
                                             const ::openmldb::api::TableMeta& aggr_meta,
                                             std::shared_ptr<Table> aggr_table, const uint32_t& index_pos,
                                             const std::string& aggr_col, const std::string& aggr_func,
                                             const std::string& ts_col, const std::string& bucket_size) {
    std::string aggr_type = boost::to_lower_copy(aggr_func);
    WindowType window_type;
    uint32_t window_size;
    if (::openmldb::base::IsNumber(bucket_size)) {
        window_type = WindowType::kRowsNum;
        window_size = std::stoi(bucket_size);
    } else {
        window_type = WindowType::kRowsRange;
        if (bucket_size.empty()) {
            PDLOG(ERROR, "Bucket size is empty");
            return std::shared_ptr<Aggregator>();
        }
        char time_unit = tolower(bucket_size.back());
        std::string time_size = bucket_size.substr(0, bucket_size.size() - 1);
        boost::trim(time_size);
        if (!::openmldb::base::IsNumber(time_size)) {
            PDLOG(ERROR, "Bucket size is not a number");
            return std::shared_ptr<Aggregator>();
        }
        switch (time_unit) {
            case 's':
                window_size = std::stoi(time_size) * 1000;
                break;
            case 'm':
                window_size = std::stoi(time_size) * 1000 * 60;
                break;
            case 'h':
                window_size = std::stoi(time_size) * 1000 * 60 * 60;
                break;
            case 'd':
                window_size = std::stoi(time_size) * 1000 * 60 * 60 * 24;
                break;
            default: {
                PDLOG(ERROR, "Unsupported time unit");
                return std::shared_ptr<Aggregator>();
            }
        }
    }

    if (aggr_type == "sum") {
        return std::make_shared<SumAggregator>(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, AggrType::kSum,
                                               ts_col, window_type, window_size);
    } else if (aggr_type == "min") {
        return std::make_shared<MinAggregator>(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, AggrType::kMin,
                                               ts_col, window_type, window_size);
    } else if (aggr_type == "max") {
        return std::make_shared<MaxAggregator>(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, AggrType::kMax,
                                               ts_col, window_type, window_size);
    } else if (aggr_type == "count") {
        return std::make_shared<CountAggregator>(base_meta, aggr_meta, aggr_table, index_pos, aggr_col,
                                                 AggrType::kCount, ts_col, window_type, window_size);
    } else if (aggr_type == "avg") {
        return std::make_shared<AvgAggregator>(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, AggrType::kAvg,
                                               ts_col, window_type, window_size);
    } else {
        PDLOG(ERROR, "Unsupported aggregate function type");
        return std::shared_ptr<Aggregator>();
    }
    return std::shared_ptr<Aggregator>();
}

}  // namespace storage
}  // namespace openmldb
