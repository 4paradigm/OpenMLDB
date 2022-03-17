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
#include "base/strings.h"
#include "common/timer.h"
#include "storage/aggregator.h"
#include "storage/table.h"

namespace openmldb {
namespace storage {

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
      window_size_(window_size) {
    for (int i = 0; i < base_meta.column_desc().size(); i++) {
        if (base_meta.column_desc(i).name() == aggr_col_) {
            aggr_col_idx_ = i;
        }
        if (base_meta.column_desc(i).name() == ts_col_) {
            ts_col_idx_ = i;
        }
    }
    aggr_col_type_ = base_meta.column_desc(aggr_col_idx_).data_type();
    auto dimension = dimensions_.Add();
    dimension->set_idx(0);
}

bool Aggregator::Update(const std::string& key, const std::string& row, const uint64_t& offset) {
    codec::RowView row_view(base_table_schema_, reinterpret_cast<int8_t*>(const_cast<char*>(row.c_str())), row.size());
    int64_t cur_ts;
    row_view.GetInt64(ts_col_idx_, &cur_ts);
    if (aggr_buffer_map_.find(key) == aggr_buffer_map_.end()) {
        aggr_buffer_map_.emplace(key, AggrBuffer{});
    }
    auto& aggr_buffer = aggr_buffer_map_.at(key);
    if (aggr_buffer.ts_begin_ == 0) {
        aggr_buffer.ts_begin_ = cur_ts;
    }
    // handle the case that the current timestamp is smaller than the begin timestamp in aggregate buffer
    if (cur_ts < aggr_buffer.ts_begin_) {
        auto it = aggr_table_->NewTraverseIterator(0);
        it->Seek(key, cur_ts);
        if (it->Valid()) {
            auto val = it->GetValue();
            std::string origin_data = val.ToString();
            codec::RowView origin_row_view(aggr_table_schema_,
                                           reinterpret_cast<int8_t*>(const_cast<char*>(origin_data.c_str())),
                                           origin_data.size());
            int32_t origin_cnt = 0;
            char* ch = NULL;
            uint32_t cn_length = 0;
            origin_row_view.GetInt32(3, &origin_cnt);
            origin_row_view.GetString(4, &ch, &cn_length);
            codec::RowBuilder row_builder(aggr_table_schema_);
            std::string new_row = origin_data;
            row_builder.SetBuffer(reinterpret_cast<int8_t*>(&(new_row[0])), new_row.size(), false);
            row_builder.SetString(0, key.c_str(), key.size());
            row_builder.SetInt32(3, origin_cnt + 1);
            row_builder.SetInt64(5, offset);
            bool ok = UpdateAggrVal(&row_view, &row_builder);
            if (!ok) {
                return false;
            }
            int64_t time = ::baidu::common::timer::get_micros() / 1000;
            dimensions_.Mutable(0)->set_key(key);
            aggr_table_->Put(time, new_row, dimensions_);
        }
        return true;
    }
    aggr_buffer.aggr_cnt_++;
    aggr_buffer.binlog_offset_ = offset;
    aggr_buffer.ts_end_ = cur_ts;
    bool ok = UpdateAggrVal(&row_view, &aggr_buffer);
    if (!ok) {
        return false;
    }

    if (window_type_ == WindowType::kRowsNum) {
        if (aggr_buffer.aggr_cnt_ >= window_size_) {
            FlushAggrBuffer(key, aggr_buffer);
        }
    } else if (window_type_ == WindowType::kRowsRange) {
        if (cur_ts - aggr_buffer.ts_begin_ >= window_size_) {
            FlushAggrBuffer(key, aggr_buffer);
        }
    } else {
        PDLOG(WARNING, "unsupported window type");
        return false;
    }

    return true;
}

void Aggregator::FlushAggrBuffer(const std::string& key, const AggrBuffer& buffer) {
    std::string encoded_row;
    codec::RowBuilder row_builder(aggr_table_schema_);
    std::string aggr_val;
    switch (aggr_col_type_) {
        case DataType::kSmallInt: {
            int16_t tmp_val = buffer.aggr_val_.vsmallint;
            aggr_val.assign(reinterpret_cast<char*>(&tmp_val), sizeof(int16_t));
            break;
        }
        case DataType::kInt: {
            int32_t tmp_val = buffer.aggr_val_.vint;
            aggr_val.assign(reinterpret_cast<char*>(&tmp_val), sizeof(int32_t));
            break;
        }
        case DataType::kBigInt: {
            int64_t tmp_val = buffer.aggr_val_.vlong;
            aggr_val.assign(reinterpret_cast<char*>(&tmp_val), sizeof(int64_t));
            break;
        }
        case DataType::kFloat: {
            float tmp_val = buffer.aggr_val_.vfloat;
            aggr_val.assign(reinterpret_cast<char*>(&tmp_val), sizeof(float));
            break;
        }
        case DataType::kDouble: {
            double tmp_val = buffer.aggr_val_.vdouble;
            aggr_val.assign(reinterpret_cast<char*>(&tmp_val), sizeof(double));
            break;
        }
        default: {
            PDLOG(WARNING, "unsupported data type");
            return;
        }
    }

    int str_length = key.size() + aggr_val.size();
    uint32_t row_size = row_builder.CalTotalLength(str_length);
    encoded_row.resize(row_size);
    row_builder.SetBuffer(reinterpret_cast<int8_t*>(&(encoded_row[0])), row_size);
    row_builder.AppendString(key.c_str(), key.size());
    row_builder.AppendTimestamp(buffer.ts_begin_);
    row_builder.AppendTimestamp(buffer.ts_end_);
    row_builder.AppendInt32(buffer.aggr_cnt_);
    row_builder.AppendString(aggr_val.c_str(), aggr_val.size());
    row_builder.AppendInt64(buffer.binlog_offset_);

    int64_t time = ::baidu::common::timer::get_micros() / 1000;
    dimensions_.Mutable(0)->set_key(key);
    aggr_table_->Put(time, encoded_row, dimensions_);

    auto& latest_val = aggr_buffer_map_.at(key);
    int64_t latest_ts = latest_val.ts_end_ + 1;
    latest_val.clear();
    latest_val.ts_begin_ = latest_ts;

    return;
}

SumAggregator::SumAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                             std::shared_ptr<Table> aggr_table, const uint32_t& index_pos, const std::string& aggr_col,
                             const AggrType& aggr_type, const std::string& ts_col, WindowType window_tpye,
                             uint32_t window_size)
    : Aggregator(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, aggr_type, ts_col, window_tpye, window_size) {}

bool SumAggregator::UpdateAggrVal(codec::RowView* row_view, codec::RowBuilder* row_builder) {
    char* ch = NULL;
    uint32_t cn_length = 0;
    switch (aggr_col_type_) {
        case DataType::kSmallInt: {
            int16_t origin_val = *reinterpret_cast<int16_t*>(ch);
            int16_t val;
            row_view->GetInt16(aggr_col_idx_, &val);
            int16_t new_val = origin_val + val;
            row_builder->SetString(4, reinterpret_cast<const char*>(&new_val), sizeof(int16_t));
            break;
        }
        case DataType::kInt: {
            int32_t origin_val = *reinterpret_cast<int32_t*>(ch);
            int32_t val;
            row_view->GetInt32(aggr_col_idx_, &val);
            int32_t new_val = origin_val + val;
            row_builder->SetString(4, reinterpret_cast<const char*>(&new_val), sizeof(int32_t));
            break;
        }
        case DataType::kBigInt: {
            int64_t origin_val = *reinterpret_cast<int64_t*>(ch);
            int64_t val;
            row_view->GetInt64(aggr_col_idx_, &val);
            int64_t new_val = origin_val + val;
            row_builder->SetString(4, reinterpret_cast<const char*>(&new_val), sizeof(int64_t));
            break;
        }
        case DataType::kFloat: {
            float origin_val = *reinterpret_cast<float*>(ch);
            float val;
            row_view->GetFloat(aggr_col_idx_, &val);
            float new_val = origin_val + val;
            row_builder->SetString(4, reinterpret_cast<const char*>(&new_val), sizeof(float));
            break;
        }
        case DataType::kDouble: {
            double origin_val = *reinterpret_cast<double*>(ch);
            double val;
            row_view->GetDouble(aggr_col_idx_, &val);
            double new_val = origin_val + val;
            row_builder->SetString(4, reinterpret_cast<const char*>(&new_val), sizeof(double));
            break;
        }
        default: {
            PDLOG(WARNING, "unsupported data type");
            return false;
        }
    }
    return true;
}

bool SumAggregator::UpdateAggrVal(codec::RowView* row_view, AggrBuffer* aggr_buffer) {
    switch (aggr_col_type_) {
        case DataType::kSmallInt: {
            int16_t val;
            row_view->GetInt16(aggr_col_idx_, &val);
            aggr_buffer->aggr_val_.vsmallint += val;
            break;
        }
        case DataType::kInt: {
            int32_t val;
            row_view->GetInt32(aggr_col_idx_, &val);
            aggr_buffer->aggr_val_.vint += val;
            break;
        }
        case DataType::kBigInt: {
            int64_t val;
            row_view->GetInt64(aggr_col_idx_, &val);
            aggr_buffer->aggr_val_.vlong += val;
            break;
        }
        case DataType::kFloat: {
            float val;
            row_view->GetFloat(aggr_col_idx_, &val);
            aggr_buffer->aggr_val_.vfloat += val;
            break;
        }
        case DataType::kDouble: {
            double val;
            row_view->GetDouble(aggr_col_idx_, &val);
            aggr_buffer->aggr_val_.vdouble += val;
            break;
        }
        default: {
            PDLOG(WARNING, "unsupported data type");
            return false;
        }
    }
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
            PDLOG(WARNING, "bucket size is empty");
            return std::shared_ptr<Aggregator>();
        }
        char time_unit = bucket_size.back();
        std::string time_size = bucket_size.substr(0, bucket_size.size() - 1);
        if (!::openmldb::base::IsNumber(time_size)) {
            PDLOG(WARNING, "bucket size is not a number");
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
                PDLOG(WARNING, "Unsupported time unit");
                return std::shared_ptr<Aggregator>();
            }
        }
    }

    if (aggr_func == "sum") {
        return std::make_shared<SumAggregator>(base_meta, aggr_meta, aggr_table, index_pos, aggr_col, AggrType::kSum,
                                               ts_col, window_type, window_size);
    } else {
        PDLOG(WARNING, "Unsupported aggregate function type");
        return std::shared_ptr<Aggregator>();
    }
    return std::shared_ptr<Aggregator>();
}

}  // namespace storage
}  // namespace openmldb
