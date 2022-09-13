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

#ifndef STREAM_SRC_EXECUTOR_KEY_PARTITION_INTERVAL_JOIN_H_
#define STREAM_SRC_EXECUTOR_KEY_PARTITION_INTERVAL_JOIN_H_

#include <glog/logging.h>

#include <memory>
#include <unordered_map>
#include <queue>
#include <string>
#include <vector>

#include "executor/interval_join.h"

namespace streaming {
namespace interval_join {

class KeyPartitionIntervalJoiner : public IntervalJoiner {
 public:
    KeyPartitionIntervalJoiner(JoinConfiguration* configuration, const std::shared_ptr<Buffer>& buffer,
                               DataStream* output, int id = 0, const Clock* clock = &Clock::GetClock())
        : IntervalJoiner(configuration, buffer, output, id, clock) {}
    int ProcessBaseElement(Element* ele) override;
    int ProcessProbeElement(Element* ele) override;

 private:
    bool DoCleanup(size_t curr_ts) override;

    std::unordered_map<std::string, std::unordered_map<int64_t, std::shared_ptr<ProcessFunction>>> processed_base_;
    std::unordered_map<std::string, std::unordered_map<int64_t, Element*>> processed_probe_;
    std::priority_queue<CleanInfo, std::vector<CleanInfo>, CleanInfoCompare> to_clean_;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_EXECUTOR_KEY_PARTITION_INTERVAL_JOIN_H_
