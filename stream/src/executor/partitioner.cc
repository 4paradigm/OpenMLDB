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

#include "executor/partitioner.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

#include <utility>

#include "common/clock.h"

namespace streaming {
namespace interval_join {

DEFINE_double(arrive_rate_per_partitioner, -1, "arrival rate of data stream per partitioner (set by main)");
DEFINE_int64(arrive_rate_ctl_unit, 1000, "the control unit of arrival rate");

Partitioner::Partitioner(DataStream* stream, PartitionStrategy* partition_strategy, int id,
                         bool latency_record, Clock* clock)
    : Executor(id), stream_(stream), partition_strategy_(partition_strategy),
      latency_record_(latency_record), clock_(clock) {}

void Partitioner::AddBuffer(const std::shared_ptr<Buffer>& buffer) { buffers_.emplace_back(buffer); }

int Partitioner::Process() {
    LOG(INFO) << "Partitioner " << id() << " Starts Running";
    auto start = clock_->GetSysTime();
    while (auto ele = stream_->Get()) {
        int pid = -1, hash = -1;
        pid = partition_strategy_->GetPartitionId(ele->key(), &hash);
        DCHECK_NE(-1, pid);
        DCHECK_NE(-1, hash);
        ele->set_pid(pid);
        ele->set_hash(hash);
        ele->set_sid(id());
        if (pid > buffers_.size()) {
            LOG(ERROR) << "partition id " << pid << " is larger than buffer size";
            return -1;
        }

        if (latency_record_) {
            ele->set_read_time(::baidu::common::timer::get_micros());
        }
        buffers_[pid]->Put(ele.get(), true);
        counter_++;
        // update the clock to the latest timestamp if ele->ts() is larger than curr_ts
        clock_->UpdateTime(ele->ts());

        if (counter_ % 100000 == 0) {
            LOG(INFO) << "[Partitioner " << id() << "] processed " << counter_ << " elements";
        }

        if (FLAGS_arrive_rate_per_partitioner > 0 && counter_ != 0 && counter_ % FLAGS_arrive_rate_ctl_unit == 0) {
            auto end = clock_->GetSysTime();
            double curr_rate = FLAGS_arrive_rate_ctl_unit / ((end - start) / 1000.0 / 1000.0);
            if (curr_rate > FLAGS_arrive_rate_per_partitioner) {
                // sleep
                int64_t sleep_us =
                    FLAGS_arrive_rate_ctl_unit * 1000 * 1000 / FLAGS_arrive_rate_per_partitioner - (end - start);
                usleep(sleep_us);
            }
            DLOG(INFO) << "partition rate: "
                       << FLAGS_arrive_rate_ctl_unit / ((clock_->GetSysTime() - start) / 1000.0 / 1000.0);
            start = end;
        }
    }
    LOG(INFO) << "[Partitioner " << id() << "] processed " << counter_ << " elements in total";
    return 0;
}

}  // namespace interval_join
}  // namespace streaming
