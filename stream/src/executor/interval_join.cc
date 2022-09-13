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

#include "executor/interval_join.h"

#include "common/count_process_function.h"
#include "common/sum_process_function.h"
#include "common/distinct_count_process_function.h"

namespace streaming {
namespace interval_join {

DEFINE_int64(joiner_timeout, 2000000, "wait time before timeout (unit: microseconds)");
DEFINE_int32(rate_output_interval, 20000, "print per how many outputs");

std::shared_ptr<ProcessFunction> IntervalJoiner::GetProcessFunction(OpType op, Element* ele) {
    switch (op) {
        case OpType::Count:
            return std::make_shared<CountProcessFunction>(ele);
        case OpType::Sum:
            return std::make_shared<SumProcessFunction>(ele);
        case OpType::DistinctCount:
            return std::make_shared<DistinctCountProcessFunction>(ele);
        default:
            return nullptr;
    }
}

int IntervalJoiner::Process() {
    LOG(INFO) << "IntervalJoiner " << id() << " Start Running";
    auto total_start = baidu::common::timer::get_micros();
    int64_t start = 0;
    int64_t end = 0;
    if (configuration_->time_record) {
        start = ::baidu::common::timer::get_micros();
    }
    int64_t local_other_ts = 0;

    while (auto ele = buffer_->Get(true, FLAGS_joiner_timeout)) {
        if (configuration_->time_record) {
            end = ::baidu::common::timer::get_micros();
            statistics_.wait_time += end - start;
            start = end;
        }

        if (stop_ || ele == nullptr) {
            break;
        }

        if (ProcessElement(ele)) {
            LOG(WARNING) << "Processed " << *ele << " failed";
        }
        counter_++;

        if (counter_ % 100000 == 0) {
            LOG(INFO) << "[Joiner " << id() << "] processed " << counter_ << " elements";
        }

        if (configuration_->time_record) {
            end = ::baidu::common::timer::get_micros();
            local_other_ts += end - start;
            start = end;
        }
    }
    end = baidu::common::timer::get_micros();
    LOG(INFO) << "[Joiner " << id() << "] Processed " << counter_ << " elements (base:" << base_counter_
              << ", probe:" << probe_counter_ << ") in total (takes " << (end - total_start) / 1000 << " ms)";
    int64_t other_ts = statistics_.match_time + statistics_.lookup_time + statistics_.output_time +
                       statistics_.cleaning_time + statistics_.clean_insert_time + statistics_.insert_time +
                       statistics_.prepare_time;
    LOG(INFO) << "Wait time: " << statistics_.wait_time << ", other: " << other_ts
              << ", local_other_ts = " << local_other_ts << ", overlap_cnt = " << overlap_cnt_;
    return 0;
}

bool IntervalJoiner::IsLate(int64_t ts) {
    return configuration_->lateness + ts < GetTime();
}

bool IntervalJoiner::IsInTimeWindow(int64_t baseTs, int64_t probeTs) {
    return baseTs - probeTs >= 0 && baseTs - probeTs <= configuration_->window;
}

int IntervalJoiner::ProcessElement(Element *ele) {
    if (configuration_->late_check) {
        if (IsLate(ele->ts())) {
            LOG(WARNING) << "Too late, ignored: " << configuration_->lateness + ele->ts() << " < " << GetTime();
            return 1;
        }
    }
    if (configuration_->cleanup) {
        Cleanup();
    }

    if (ele->type() == ElementType::kBase) {
        DLOG(INFO) << "[" << id() << "] " << "ProcessBaseElement " << ele->ToString();
        base_counter_++;
        ProcessBaseElement(ele);
    } else if (ele->type() == ElementType::kProbe) {
        DLOG(INFO) << "[" << id() << "] " << "ProcessProbeElement " << ele->ToString();
        probe_counter_++;
        ProcessProbeElement(ele);
    } else {
        LOG(ERROR) << "Unsupport element type";
        return -1;
    }
    if (configuration_->latency_record) {
        ele->set_output_time(baidu::common::timer::get_micros());
    }
    return 0;
}

bool IntervalJoiner::Cleanup() {
    // hasn't reached cleanup time interval
    size_t curr_time = GetSysTime();
    if (configuration_->cleanup_interval == -1 || curr_time - last_time_ < configuration_->cleanup_interval) {
        return false;
    }
    last_time_ = curr_time;

    return DoCleanup(GetTime());
}

}  // namespace interval_join
}  // namespace streaming
