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
#ifndef STREAM_SRC_EXECUTOR_INTERVAL_JOIN_H_
#define STREAM_SRC_EXECUTOR_INTERVAL_JOIN_H_

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>
#include <utility>

#include "common/clock.h"
#include "common/buffer.h"
#include "common/process_function.h"
#include "common/clean_info.h"
#include "common/join_config.h"
#include "executor/executor.h"
#include "stream/data_stream.h"
#include "gtest/gtest_prod.h"
#include "common/time_statistics.h"

namespace streaming {
namespace interval_join {

DECLARE_int64(joiner_timeout);
DECLARE_int32(rate_output_interval);

enum class JoinStrategy {
    kKeyPartition,
    kDynamic,
};

class IntervalJoiner : public Executor {
 public:
    IntervalJoiner(const JoinConfiguration* configuration, const std::shared_ptr<Buffer>& buffer, DataStream* output,
                   int id = 0, const Clock* clock = &Clock::GetClock())
        : Executor(id), buffer_(buffer), output_(output), configuration_(configuration), clock_(clock) {
        last_time_out_ = clock_->GetSysTime();
    }

    static std::shared_ptr<ProcessFunction> GetProcessFunction(OpType op, Element* ele);
    int Process() override;
    // we not mark as `const Element*` in case we may do some update on the stats of Element
    int ProcessElement(Element* ele);
    virtual int ProcessBaseElement(Element* ele) = 0;
    virtual int ProcessProbeElement(Element* ele) = 0;
    bool Cleanup();

    bool IsInTimeWindow(int64_t baseTs, int64_t probeTs);
    bool IsLate(int64_t ts);

    const TimeStatistics& statistics() const { return statistics_;}

    // for testing only
    size_t GetProcessedCounter() const { return counter_; }

 protected:
    inline size_t GetTime() const {
        return clock_->GetTime();
    }

    inline size_t GetSysTime() const {
        return clock_->GetSysTime();
    }

    void Output(const Element* ele) {
        output_->Put(ele);
    }

    void Output(Element&& ele) {
        output_->Put(std::move(ele));
        auto out_counter = out_counter_++;
        if (out_counter != 0 && out_counter % FLAGS_rate_output_interval == 0) {
            auto curr_time = clock_->GetSysTime();
            LOG(INFO) << "Output thr: "
                      << (static_cast<double>(FLAGS_rate_output_interval) / ((curr_time - last_time_out_) / 1000.0))
                      << " K records/second";
            last_time_out_ = curr_time;
        }
    }

    std::shared_ptr<Buffer> buffer_;
    size_t counter_ = 0;
    DataStream* output_ = nullptr;
    const JoinConfiguration* configuration_ = nullptr;
    const Clock* clock_ = nullptr;
    TimeStatistics statistics_;
    std::atomic<int64_t> overlap_cnt_ = 0;

 private:
    virtual bool DoCleanup(size_t curr_ts) = 0;

    size_t last_time_ = 0;
    int64_t base_counter_ = 0;
    int64_t probe_counter_ = 0;
    std::atomic<int64_t> out_counter_ = 0;
    size_t last_time_out_ = 0;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_EXECUTOR_INTERVAL_JOIN_H_
