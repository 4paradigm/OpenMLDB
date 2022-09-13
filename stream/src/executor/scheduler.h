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
#ifndef STREAM_SRC_EXECUTOR_SCHEDULER_H_
#define STREAM_SRC_EXECUTOR_SCHEDULER_H_

#include "common/clock.h"
#include "executor/executor.h"
#include "executor/partition_strategy.h"

namespace streaming {
namespace interval_join {

class Scheduler : public Executor {
 public:
    explicit Scheduler(PartitionStrategy* partition_strategy, int id = 0, const Clock* clock = &Clock::GetClock())
        : Executor(id), partition_strategy_(partition_strategy), clock_(clock) {}
    ~Scheduler() override {}

 protected:
    int Process() override;

 private:
    PartitionStrategy* partition_strategy_ = nullptr;
    size_t last_update_time_ = 0;
    const Clock* clock_ = nullptr;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_EXECUTOR_SCHEDULER_H_
