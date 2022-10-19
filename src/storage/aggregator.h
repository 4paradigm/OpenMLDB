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

#ifndef SRC_STORAGE_AGGREGATOR_H_
#define SRC_STORAGE_AGGREGATOR_H_

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "codec/codec.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "replica/log_replicator.h"
#include "storage/table.h"

namespace openmldb {
namespace storage {

using Dimensions = google::protobuf::RepeatedPtrField<::openmldb::api::Dimension>;
using ::openmldb::log::LogParts;
using ::openmldb::replica::LogReplicator;
using ::openmldb::type::DataType;

enum class AggrType {
    kSum = 1,
    kMin = 2,
    kMax = 3,
    kCount = 4,
    kAvg = 5,
};

enum class WindowType {
    kRowsNum = 1,
    kRowsRange = 2,
};

enum class AggrStat {
    kUnInit = 1,
    kRecovering = 2,
    kInited = 3,
};

class AggrBuffer {
 public:
    union AggrVal {
        int16_t vsmallint;
        int32_t vint;
        int64_t vlong;
        float vfloat;
        double vdouble;
        struct {
            uint32_t len;
            char* data;
        } vstring;
    } aggr_val_;
    int64_t ts_begin_;
    int64_t ts_end_;
    uint64_t binlog_offset_;
    int64_t non_null_cnt_;
    int32_t aggr_cnt_;
    DataType data_type_;
    AggrBuffer() : aggr_val_(), ts_begin_(-1), ts_end_(0), binlog_offset_(0), non_null_cnt_(0), aggr_cnt_(0) {}
    AggrBuffer(const AggrBuffer& buffer) {
        memcpy(&aggr_val_, &buffer.aggr_val_, sizeof(aggr_val_));
        ts_begin_ = buffer.ts_begin_;
        ts_end_ = buffer.ts_end_;
        aggr_cnt_ = buffer.aggr_cnt_;
        binlog_offset_ = buffer.binlog_offset_;
        non_null_cnt_ = buffer.non_null_cnt_;
        data_type_ = buffer.data_type_;
        if (data_type_ == DataType::kString || data_type_ == DataType::kVarchar) {
            if (buffer.aggr_val_.vstring.data != NULL) {
                aggr_val_.vstring.data = new char[buffer.aggr_val_.vstring.len];
                memcpy(aggr_val_.vstring.data, buffer.aggr_val_.vstring.data, buffer.aggr_val_.vstring.len);
            }
        }
    }
    AggrBuffer& operator=(const AggrBuffer& buffer) = delete;
    ~AggrBuffer() { clear(); }
    void clear() {
        if (data_type_ == DataType::kString || data_type_ == DataType::kVarchar) {
            if (aggr_val_.vstring.data != NULL) {
                delete[] aggr_val_.vstring.data;
            }
        }
        memset(&aggr_val_, 0, sizeof(aggr_val_));
        ts_begin_ = -1;
        ts_end_ = 0;
        aggr_cnt_ = 0;
        binlog_offset_ = 0;
        non_null_cnt_ = 0;
    }
    bool AggrValEmpty() const { return non_null_cnt_ == 0; }

    bool IsInited() const {
        return ts_begin_ != -1;
    }
};
struct AggrBufferLocked {
    std::unique_ptr<std::mutex> mu_;
    AggrBuffer buffer_;
    AggrBufferLocked() : mu_(std::make_unique<std::mutex>()), buffer_() {}
};

class Aggregator {
 public:
    Aggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
               std::shared_ptr<Table> aggr_table, std::shared_ptr<LogReplicator> aggr_replicator,
               const uint32_t& index_pos, const std::string& aggr_col, const AggrType& aggr_type,
               const std::string& ts_col, WindowType window_tpye, uint32_t window_size);

    ~Aggregator();

    bool Update(const std::string& key, const std::string& row, const uint64_t& offset, bool recover = false,
                bool reverse = false);

    bool DeleteAndUpdate(const std::string& key, const std::string& row,
                         const uint64_t& offset, bool recover = false);

    bool Delete(const std::string& key);

    bool FlushAll();

    bool Init(std::shared_ptr<LogReplicator> base_replicator);

    void SetReletedTable(std::shared_ptr<Table> base_table);

    uint32_t GetIndexPos() const { return index_pos_; }

    AggrType GetAggrType() const { return aggr_type_; }

    DataType GetAggrColType() const { return aggr_col_type_; }

    WindowType GetWindowType() const { return window_type_; }

    uint32_t GetWindowSize() const { return window_size_; }

    AggrStat GetStat() const { return status_.load(std::memory_order_relaxed); }

    bool GetAggrBuffer(const std::string& key, AggrBuffer** buffer);

    bool GetAggrBuffer(const std::string& key, const std::string& filter_key, AggrBuffer** buffer);

    uint32_t GetAggrTid() { return aggr_table_->GetId(); }

    // set the filter column info that not initialized in constructor
    bool SetFilter(absl::string_view filter_col);

 protected:
    codec::Schema base_table_schema_;
    codec::Schema aggr_table_schema_;

    using FilterMap = std::unordered_map<std::string, AggrBufferLocked>;  // filter_column -> aggregator buffer
    std::unordered_map<std::string, FilterMap> aggr_buffer_map_;          // key -> filter_map
    std::mutex mu_;
    DataType aggr_col_type_;
    DataType ts_col_type_;
    std::shared_ptr<LogReplicator> base_replicator_;
    std::shared_ptr<Table> aggr_table_;
    std::shared_ptr<LogReplicator> aggr_replicator_;
    std::atomic<AggrStat> status_;

    std::shared_ptr<Table> base_table_;

    bool GetAggrBufferFromRowView(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* buffer);
    bool FlushAggrBuffer(const std::string& key, const std::string& filter_key, const AggrBuffer& aggr_buffer);
    bool UpdateFlushedBuffer(const std::string& key, const std::string& filter_key, const int8_t* base_row_ptr,
                             int64_t cur_ts, uint64_t offset, bool reverse);
    bool CheckBufferFilled(int64_t cur_ts, int64_t buffer_end, int32_t buffer_cnt);

 private:
    virtual bool UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer,
                               bool reverse) = 0;
    virtual bool EncodeAggrVal(const AggrBuffer& buffer, std::string* aggr_val) = 0;
    virtual bool DecodeAggrVal(const int8_t* row_ptr, AggrBuffer* buffer) = 0;
    virtual bool UpdateAggrValAfterDeleteRange(const codec::RowView& row_view, const int8_t* row_ptr,
                                       AggrBuffer* aggr_buffer) = 0;
    int64_t AlignedStart(int64_t ts) {
        if (window_type_ == WindowType::kRowsRange) {
            return ts / window_size_ * window_size_;
        } else {
            return ts;
        }
    }

    uint32_t index_pos_;
    uint32_t aggr_index_pos_ = 0;
    std::string aggr_col_;
    AggrType aggr_type_;
    std::string ts_col_;

 protected:
    int aggr_col_idx_;
    int ts_col_idx_;
    std::string filter_col_;
    int filter_col_idx_;
    WindowType window_type_;

    // for kRowsNum, window_size_ is the rows num in mini window
    // for kRowsRange, window size is the time interval in mini window
    int32_t window_size_;

    codec::RowView base_row_view_;
    codec::RowView aggr_row_view_;
    codec::RowBuilder row_builder_;
};

class SumAggregator : public Aggregator {
 public:
    SumAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                  std::shared_ptr<Table> aggr_table, std::shared_ptr<LogReplicator> aggr_replicator,
                  const uint32_t& index_pos, const std::string& aggr_col, const AggrType& aggr_type,
                  const std::string& ts_col, WindowType window_tpye, uint32_t window_size);

    ~SumAggregator() = default;

 private:
    bool UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer,
                       bool reverse) override;

    bool EncodeAggrVal(const AggrBuffer& buffer, std::string* aggr_val) override;

    bool DecodeAggrVal(const int8_t* row_ptr, AggrBuffer* buffer) override;

    bool UpdateAggrValAfterDeleteRange(const codec::RowView& row_view, const int8_t* row_ptr,
                                       AggrBuffer* aggr_buffer) override;
};

class MinMaxBaseAggregator : public Aggregator {
 public:
    MinMaxBaseAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                         std::shared_ptr<Table> aggr_table, std::shared_ptr<LogReplicator> aggr_replicator,
                         const uint32_t& index_pos, const std::string& aggr_col, const AggrType& aggr_type,
                         const std::string& ts_col, WindowType window_tpye, uint32_t window_size);

    ~MinMaxBaseAggregator() = default;

 private:
    bool EncodeAggrVal(const AggrBuffer& buffer, std::string* aggr_val) override;

    bool DecodeAggrVal(const int8_t* row_ptr, AggrBuffer* buffer) override;

    bool UpdateAggrValAfterDeleteRange(const codec::RowView& row_view, const int8_t* row_ptr,
                                       AggrBuffer* aggr_buffer) override;
};
class MinAggregator : public MinMaxBaseAggregator {
 public:
    MinAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                  std::shared_ptr<Table> aggr_table, std::shared_ptr<LogReplicator> aggr_replicator,
                  const uint32_t& index_pos, const std::string& aggr_col, const AggrType& aggr_type,
                  const std::string& ts_col, WindowType window_tpye, uint32_t window_size);

    ~MinAggregator() = default;

 private:
    bool UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer,
                       bool reverse) override;

    bool UpdateAggrValAfterDeleteRange(const codec::RowView& row_view, const int8_t* row_ptr,
                                       AggrBuffer* aggr_buffer) override;
};

class MaxAggregator : public MinMaxBaseAggregator {
 public:
    MaxAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                  std::shared_ptr<Table> aggr_table, std::shared_ptr<LogReplicator> aggr_replicator,
                  const uint32_t& index_pos, const std::string& aggr_col, const AggrType& aggr_type,
                  const std::string& ts_col, WindowType window_tpye, uint32_t window_size);

    ~MaxAggregator() = default;

 private:
    bool UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer,
                       bool reverse) override;

    bool UpdateAggrValAfterDeleteRange(const codec::RowView& row_view, const int8_t* row_ptr,
                                       AggrBuffer* aggr_buffer) override;
};

class CountAggregator : public Aggregator {
 public:
    CountAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                    std::shared_ptr<Table> aggr_table, std::shared_ptr<LogReplicator> aggr_replicator,
                    const uint32_t& index_pos, const std::string& aggr_col, const AggrType& aggr_type,
                    const std::string& ts_col, WindowType window_tpye, uint32_t window_size);

    ~CountAggregator() = default;

 private:
    bool UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer,
                       bool reverse) override;

    bool EncodeAggrVal(const AggrBuffer& buffer, std::string* aggr_val) override;

    bool DecodeAggrVal(const int8_t* row_ptr, AggrBuffer* buffer) override;

    bool UpdateAggrValAfterDeleteRange(const codec::RowView& row_view, const int8_t* row_ptr,
                                       AggrBuffer* aggr_buffer) override;

    bool count_all = false;
};

class AvgAggregator : public Aggregator {
 public:
    AvgAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                  std::shared_ptr<Table> aggr_table, std::shared_ptr<LogReplicator> aggr_replicator,
                  const uint32_t& index_pos, const std::string& aggr_col, const AggrType& aggr_type,
                  const std::string& ts_col, WindowType window_tpye, uint32_t window_size);

    ~AvgAggregator() = default;

 private:
    bool UpdateAggrVal(const codec::RowView& row_view, const int8_t* row_ptr, AggrBuffer* aggr_buffer,
                       bool reverse) override;

    bool EncodeAggrVal(const AggrBuffer& buffer, std::string* aggr_val) override;

    bool DecodeAggrVal(const int8_t* row_ptr, AggrBuffer* buffer) override;

    bool UpdateAggrValAfterDeleteRange(const codec::RowView& row_view, const int8_t* row_ptr,
                                       AggrBuffer* aggr_buffer) override;
};

std::shared_ptr<Aggregator> CreateAggregator(const ::openmldb::api::TableMeta& base_meta,
                                             const ::openmldb::api::TableMeta& aggr_meta,
                                             std::shared_ptr<Table> aggr_table,
                                             std::shared_ptr<LogReplicator> aggr_replicator, const uint32_t& index_pos,
                                             const std::string& aggr_col, const std::string& aggr_func,
                                             const std::string& ts_col, const std::string& bucket_size,
                                             const std::string& filter_col = "");

using Aggrs = std::vector<std::shared_ptr<Aggregator>>;
}  // namespace storage
}  // namespace openmldb

#endif  // SRC_STORAGE_AGGREGATOR_H_
