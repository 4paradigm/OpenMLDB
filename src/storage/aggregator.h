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
#include <string>
#include <unordered_map>
#include <vector>

#include "codec/codec.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "storage/table.h"

namespace openmldb {
namespace storage {

using Dimensions = google::protobuf::RepeatedPtrField<::openmldb::api::Dimension>;
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

class Aggregator {
 public:
    Aggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
               std::shared_ptr<Table> aggr_table, const uint32_t& index_pos, const std::string& aggr_col,
               const AggrType& aggr_type, const std::string& ts_col, WindowType window_tpye, uint32_t window_size);

    ~Aggregator() = default;

    bool Update(const std::string& key, const std::string& row, const uint64_t& offset);

    uint32_t GetIndexPos() const { return index_pos_; }

 protected:
    codec::Schema base_table_schema_;
    codec::Schema aggr_table_schema_;
    int aggr_col_idx_;
    int ts_col_idx_;
    union AggrVal {
        int16_t vsmallint;
        int32_t vint;
        int64_t vlong;
        float vfloat;
        double vdouble;
    };

    struct AggrBuffer {
        AggrVal aggr_val_;
        int64_t ts_begin_;
        int64_t ts_end_;
        int32_t aggr_cnt_;
        uint64_t binlog_offset_;
        AggrBuffer() : aggr_val_(), ts_begin_(0), ts_end_(0), aggr_cnt_(0), binlog_offset_(0) {}
        void clear() { memset(this, 0, sizeof(AggrBuffer)); }
    };

    std::unordered_map<std::string, AggrBuffer> aggr_buffer_map_;
    DataType aggr_col_type_;
    std::shared_ptr<Table> aggr_table_;
    Dimensions dimensions_;

    void FlushAggrBuffer(const std::string& key, const AggrBuffer& aggr_buffer);

 private:
    virtual bool UpdateAggrVal(codec::RowView* row_view, codec::RowBuilder* row_builder) { return false; }
    virtual bool UpdateAggrVal(codec::RowView* row_view, AggrBuffer* aggr_buffer) { return false; }

    uint32_t index_pos_;
    std::string aggr_col_;
    AggrType aggr_type_;
    std::string ts_col_;

 protected:
    WindowType window_type_;
    int32_t window_size_;
};

class SumAggregator : public Aggregator {
 public:
    SumAggregator(const ::openmldb::api::TableMeta& base_meta, const ::openmldb::api::TableMeta& aggr_meta,
                  std::shared_ptr<Table> aggr_table, const uint32_t& index_pos, const std::string& aggr_col,
                  const AggrType& aggr_type, const std::string& ts_col, WindowType window_tpye, uint32_t window_size);

    ~SumAggregator() = default;

 private:
    bool UpdateAggrVal(codec::RowView* row_view, codec::RowBuilder* row_builder) override;
    bool UpdateAggrVal(codec::RowView* row_view, AggrBuffer* aggr_buffer) override;
};

std::shared_ptr<Aggregator> CreateAggregator(const ::openmldb::api::TableMeta& base_meta,
                                             const ::openmldb::api::TableMeta& aggr_meta,
                                             std::shared_ptr<Table> aggr_table, const uint32_t& index_pos,
                                             const std::string& aggr_col, const std::string& aggr_func,
                                             const std::string& ts_col, const std::string& bucket_size);

using Aggrs = std::vector<std::shared_ptr<Aggregator>>;
}  // namespace storage
}  // namespace openmldb

#endif  // SRC_STORAGE_AGGREGATOR_H_
