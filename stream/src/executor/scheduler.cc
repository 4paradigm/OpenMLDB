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

#include "executor/scheduler.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

namespace streaming {
namespace interval_join {

DEFINE_int64(schedule_update_interval_us, 10000, "the interval to update partition schedule");
DEFINE_bool(schedule_update, true, "whether update partition schedule");

int Scheduler::Process() {
    while (FLAGS_schedule_update && !stop_) {
        int64_t curr_ts = clock_->GetSysTime();
        if (curr_ts - last_update_time_ >= FLAGS_schedule_update_interval_us) {
            DLOG(INFO) << "Update partition schedule";
            partition_strategy_->UpdateSchedule();
            last_update_time_ = curr_ts;
        }
    }
    LOG(INFO) << "Scheduler done";
    return 0;
}

}  // namespace interval_join
}  // namespace streaming
