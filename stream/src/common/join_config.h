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

#ifndef STREAM_SRC_COMMON_JOIN_CONFIG_H_
#define STREAM_SRC_COMMON_JOIN_CONFIG_H_

#include "common/process_function.h"
#include "common/clock.h"

namespace streaming {
namespace interval_join {

struct JoinConfiguration {
    JoinConfiguration(int64_t window, int64_t lateness, int64_t cleanup_interval, OpType op, bool cleanup = true,
                      bool late_check = true, bool time_record = false, bool latency_record = false,
                      bool overlapping = false, bool overlapping_record = false, bool effectiveness_record = false)
        : window(window),
          lateness(lateness),
          cleanup_interval(cleanup_interval),
          cleanup(cleanup),
          late_check(late_check),
          time_record(time_record),
          latency_record(latency_record),
          overlapping(overlapping),
          overlapping_record(overlapping_record),
          effectiveness_record(effectiveness_record),
          op(op) {}

    const int64_t window;
    const int64_t lateness;
    const int64_t cleanup_interval;
    bool cleanup = true;
    bool late_check = true;
    bool time_record = false;
    bool latency_record = false;
    bool overlapping = false;
    bool overlapping_record = false;
    bool effectiveness_record = false;
    const OpType op;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_COMMON_JOIN_CONFIG_H_
