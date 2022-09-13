/*
 *  Copyright 2021 4Paradigm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef STREAM_SRC_EXECUTOR_PARTITIONER_H_
#define STREAM_SRC_EXECUTOR_PARTITIONER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "executor/executor.h"
#include "executor/interval_join.h"
#include "executor/partition_strategy.h"
#include "stream/data_stream.h"

namespace streaming {
namespace interval_join {

DECLARE_double(arrive_rate_per_partitioner);

class Partitioner : public Executor {
 public:
    explicit Partitioner(DataStream* stream, PartitionStrategy* partition_strategy, int id = 0,
                         bool latency_record = false, Clock* clock = &Clock::GetClock());
    int Process() override;

    void AddBuffer(const std::shared_ptr<Buffer>& buffer);

    size_t GetProcessedCounter() const { return counter_; }

 private:
    DataStream* stream_ = nullptr;
    std::vector<std::shared_ptr<Buffer>> buffers_;
    size_t counter_ = 0;
    PartitionType partition_type_ = PartitionType::kHash;
    PartitionStrategy* partition_strategy_ = nullptr;
    bool latency_record_;
    Clock* clock_;
    int64_t last_schedule_update_ = 0;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_EXECUTOR_PARTITIONER_H_
