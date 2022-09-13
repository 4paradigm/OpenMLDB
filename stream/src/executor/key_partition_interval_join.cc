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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <common/timer.h>

#include <string>
#include <utility>

#include "executor/key_partition_interval_join.h"
#include "common/clock.h"
#include "common/count_process_function.h"
#include "common/process_function.h"

namespace streaming {
namespace interval_join {

int KeyPartitionIntervalJoiner::ProcessBaseElement(Element *ele) {
    DCHECK(ElementType::kBase == ele->type());
    int64_t start;
    int64_t end;
    if (configuration_->time_record) {
        start = ::baidu::common::timer::get_micros();
    }

    const std::string& key = ele->key();
    int64_t ts = ele->ts();

    auto function = GetProcessFunction(configuration_->op, ele);
    if (function == nullptr) {
        LOG(ERROR) << "Unsupported op type";
        return -1;
    }
    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.prepare_time += end - start;
        start = end;
    }

    double visited = 0;
    double joined = 0;
    auto probe_it = processed_probe_.find(key);
    if (probe_it != processed_probe_.end()) {
        auto& probe_list = probe_it->second;
        for (auto& it : probe_list) {
            if (configuration_->effectiveness_record) {
                visited++;
            }
            if (configuration_->time_record) {
                end = ::baidu::common::timer::get_micros();
                statistics_.lookup_time += end - start;
                start = end;
            }

            if (IsInTimeWindow(ts, it.first)) {
                function->AddElement(it.second);

                if (configuration_->effectiveness_record) {
                    joined++;
                }
                if (configuration_->overlapping_record) {
                    it.second->increment_join_count();
                }
                if (configuration_->time_record) {
                    end = ::baidu::common::timer::get_micros();
                    statistics_.match_time += end - start;
                    start = end;
                }
            }
        }
    }

    if (visited != 0 && configuration_->effectiveness_record) {
        ele->set_effectiveness(joined / visited);
    }
    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.lookup_time += end - start;
        start = end;
    }

    // output result
    Element output_ele(key, function->GetResult(), function->GetTs());
    Output(std::move(output_ele));

    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.output_time += end - start;
        start = end;
    }

    // add element into processed base list
    auto base_it = processed_base_.find(key);
    if (base_it == processed_base_.end()) {
        base_it = processed_base_
                .emplace(std::make_pair(key, std::unordered_map<int64_t, std::shared_ptr<ProcessFunction>>()))
                .first;
    }
    auto& keyed_map = base_it->second;
    // TODO(xinyi): elements with same key and ts
    if (keyed_map.find(ts) == keyed_map.end()) {
        keyed_map.emplace(std::make_pair(ts, function));
    } else {
        LOG(ERROR) << "keyed map already has elements with ts " + std::to_string(ts);
    }

    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.insert_time += end - start;
        start = end;
    }

    if (configuration_->cleanup) {
        to_clean_.emplace(CleanInfo(ts, key, ts, true));

        if (configuration_->time_record) {
            end = ::baidu::common::timer::get_micros();
            statistics_.clean_insert_time += end - start;
            start = end;
        }
    }
    return 0;
}

int KeyPartitionIntervalJoiner::ProcessProbeElement(Element *ele) {
    DCHECK(ElementType::kProbe == ele->type());
    int64_t start;
    int64_t end;
    if (configuration_->time_record) {
        start = ::baidu::common::timer::get_micros();
    }

    const std::string& key = ele->key();
    int64_t ts = ele->ts();

    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.prepare_time += end - start;
        start = end;
    }

    double visited = 0;
    double joined = 0;
    auto base_it = processed_base_.find(key);
    if (base_it != processed_base_.end()) {
        auto& base_list = base_it->second;
        for (auto& it : base_list) {
            if (configuration_->effectiveness_record) {
                visited++;
            }
            if (configuration_->time_record) {
                end = ::baidu::common::timer::get_micros();
                statistics_.lookup_time += end - start;
                start = end;
            }

            if (IsInTimeWindow(it.first, ts)) {
                ProcessFunction* function = it.second.get();
                function->AddElement(ele);

                if (configuration_->effectiveness_record) {
                    joined++;
                }
                if (configuration_->overlapping_record) {
                    ele->increment_join_count();
                }
                if (configuration_->time_record) {
                    end = ::baidu::common::timer::get_micros();
                    statistics_.match_time += end - start;
                    start = end;
                }

                Element output_ele(key, function->GetResult(), function->GetTs());
                Output(std::move(output_ele));

                if (configuration_->time_record) {
                    end = ::baidu::common::timer::get_micros();
                    statistics_.output_time += end - start;
                    start = end;
                }
            }
        }
    }

    if (visited != 0 && configuration_->effectiveness_record) {
        ele->set_effectiveness(joined / visited);
    }
    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.lookup_time += end - start;
        start = end;
    }

    // add element into processed probe list
    auto probe_it = processed_probe_.find(key);
    if (probe_it == processed_probe_.end()) {
        probe_it = processed_probe_
                .emplace(std::make_pair(key, std::unordered_map<int64_t, Element*>()))
                .first;
    }
    auto& keyed_map = probe_it->second;
    // TODO(xinyi): elements with same key and ts
    if (keyed_map.find(ts) == keyed_map.end()) {
        keyed_map.emplace(std::make_pair(ts, ele));
    } else {
        LOG(ERROR) << "keyed map already has elements with ts " + std::to_string(ts);
    }

    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.insert_time += end - start;
        start = end;
    }

    if (configuration_->cleanup) {
        to_clean_.emplace(CleanInfo(ts + configuration_->window, key, ts, false));
        if (configuration_->time_record) {
            end = ::baidu::common::timer::get_micros();
            statistics_.clean_insert_time += end - start;
            start = end;
        }
    }

    return 0;
}

bool KeyPartitionIntervalJoiner::DoCleanup(size_t curr_ts) {
    int64_t start;
    int64_t end;
    if (configuration_->time_record) {
        start = ::baidu::common::timer::get_micros();
    }

    while (!to_clean_.empty()) {
        const CleanInfo& info = to_clean_.top();
        if (info.clean_ts() + configuration_->lateness >= curr_ts) {
            break;
        }
        if (info.is_base()) {
            processed_base_.at(info.key()).erase(info.ts());
            DLOG(INFO) << "cleanup base " << info.key() << "|" << info.ts();
         } else {
            processed_probe_.at(info.key()).erase(info.ts());
            DLOG(INFO) << "cleanup probe " << info.key() << "|" << info.ts();
        }
        to_clean_.pop();
    }

    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.cleaning_time += end - start;
        start = end;
    }

    return true;
}

}  // namespace interval_join
}  // namespace streaming
