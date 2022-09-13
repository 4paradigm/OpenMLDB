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

#include <glog/logging.h>

#include <memory>
#include <utility>

#include "executor/single_thread_executor.h"
#include "executor/interval_join.h"

namespace streaming {
namespace interval_join {

int SingleThreadExecutor::Process() {
    LOG(INFO) << "single thread executor starts running";
    for (auto& ele : input_->elements()) {
        if (ele->type() == ElementType::kBase) {
            ProcessBaseElement(ele.get());
        } else if (ele->type() == ElementType::kProbe) {
            ProcessProbeElement(ele.get());
        } else {
            LOG(ERROR) << "unsupported element type";
            return -1;
        }
    }
    return 0;
}

int SingleThreadExecutor::ProcessBaseElement(Element* ele) {
    const std::string& key = ele->key();
    int64_t ts = ele->ts();
    std::shared_ptr<ProcessFunction> function = IntervalJoiner::GetProcessFunction(configuration_->op, ele);

    auto probe_it = processed_probe_.find(key);
    if (probe_it != processed_probe_.end()) {
        auto& keyed_probe = probe_it->second;
        int64_t earliest_ts = ts - configuration_->window;
        // remove outdated probe elements
        while (!keyed_probe.empty() && keyed_probe.front()->ts() < earliest_ts) {
            keyed_probe.pop_front();
        }
        // add probe elements in time window
        for (auto ele_it : keyed_probe) {
            if (ele_it->ts() > ts) {
                break;
            }
            function->AddElement(ele_it);
        }
    }

    // output result
    Element output_ele(key, function->GetResult(), ts);
    output_->Put(std::move(output_ele));
    return 0;
}

int SingleThreadExecutor::ProcessProbeElement(Element* ele) {
    const std::string& key = ele->key();
    auto probe_it = processed_probe_.find(key);
    // no out-of-order case --> can store as a deque directly
    if (probe_it == processed_probe_.end()) {
        probe_it = processed_probe_.emplace(std::make_pair(key, std::deque<Element*>())).first;
    }
    auto& keyed_list = probe_it->second;
    if ((!keyed_list.empty()) && keyed_list.back()->ts() > ele->ts()) {
        LOG(ERROR) << "probe elements from stream are out-of-order";
        return -1;
    }
    keyed_list.push_back(ele);
    return 0;
}

}  // namespace interval_join
}  // namespace streaming

