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

#include "executor/dynamic_interval_join.h"

#include <gflags/gflags.h>
#include <string>
#include <utility>

namespace streaming {
namespace interval_join {

DEFINE_int64(
    overlapping_backoff, 100,
    "how long (in microseconds) to back-off to get a steady old base function (steady means no update will happen)");

int DynamicIntervalJoiner::ProcessBaseElement(Element *ele) {
    DCHECK(id() == ele->pid());
    DCHECK_NE(ele->hash(), -1);
    int64_t start;
    int64_t end;
    if (configuration_->time_record) {
        start = ::baidu::common::timer::get_micros();
    }
    const std::string& key = ele->key();
    int64_t ts = ele->ts();
    std::vector<int> pids = partition_strategy_->GetPartitionSet(ele->hash());
    Ticket ticket;
    std::shared_ptr<ProcessFunction> function = nullptr;
    int64_t start_ts = 0;
    std::string old_val;
    ProcessFunction* old_neighbor_func = nullptr;
    Slice key_slice(key.data(), key.size());
    if (configuration_->overlapping) {
        // std::vector<MemTableIterator> probe_iters;
        std::vector<MemTableIterator*> base_iters;
        for (auto pid : pids) {
            // probe_iters.emplace_back(peers_.at(pid)->GetProbeTable().NewIterator(key, ticket));
            base_iters.emplace_back(peers_.at(pid)->GetBaseTable().NewIterator(key_slice, ticket));
        }
        // CombinedIterator probe_it(probe_iters);
        CombinedIterator base_it(base_iters);

        // [stale_start, stale_end] should be cleared from the function
        int64_t stale_start = -1;
        int64_t stale_end = -1;
        int64_t neighbor_search_ts = ts - FLAGS_overlapping_backoff;
        if (!base_it.Null() && neighbor_search_ts >= 0) {
            base_it.Seek(neighbor_search_ts);
            if (base_it.Valid()) {
                const Element* neighbor = reinterpret_cast<const Element*>(base_it.GetRawValue());
                int64_t neighbor_ts = neighbor->ts();
                DCHECK(neighbor_ts < ts);
                if (IsInTimeWindow(ts, neighbor_ts) && neighbor->func()) {
                    // FIXME(zhanghao): there is a thread-safe issues.
                    // If there are probe elements whose ts <= start_ts, but is not yet applied in the neighbor func,
                    // then these probe elements may not be applied in the current element
                    start_ts = neighbor_ts + 1;
                    stale_start = neighbor_ts - configuration_->window;
                    stale_end = ts - configuration_->window - 1;

                    auto neighbor_func = dynamic_cast<ProcessFunction*>(neighbor->func());
                    function = neighbor_func->Clone();
                    function->SetBase(ele);
                    function->set_start_ts(start_ts);
                    old_val = function->GetResultAtomic();
                    old_neighbor_func = neighbor_func;
                    DLOG(INFO) << ele->ToString() << " base " << neighbor->ToString()
                               << "， func val: " << function->GetResultAtomic();
                    overlap_cnt_++;
                }
            }
        }
        ticket.Pop();

        if (stale_end >= 0) {
            for (auto pid : pids) {
                auto probe_it = peers_.at(pid)->GetProbeTable().NewIterator(key, ticket);
                if (probe_it) {
                    probe_it->Seek(stale_end);
                    while (probe_it->Valid()) {
                        const Element* other = reinterpret_cast<const Element*>(probe_it->GetValue().data());
                        int64_t curr_ts = other->ts();
                        if (curr_ts > stale_end) {
                            LOG(WARNING) << "Access Probe elements Race happens. curr_ts = " << curr_ts
                                         << ", stale_end = " << stale_end;
                            probe_it->Next();
                            continue;
                        } else if (curr_ts < stale_start) {
                            break;
                        } else {
                            function->RemoveElement(other);
                        }

                        probe_it->Next();
                    }
                }
                ticket.Pop();
            }
        }
        for (int i = 0; i < pids.size(); i++) {
            ticket.Pop();
            // ticket.Pop();
        }
    }

    auto new_function = GetProcessFunction(configuration_->op, ele);
    if (new_function == nullptr) {
        LOG(ERROR) << "Unsupported op type";
        return -1;
    }
    if (function == nullptr) {
        function = new_function;
    }

    ele->set_func(function);
    if (old_neighbor_func != nullptr) {
        if (old_neighbor_func->GetResultAtomic().compare(old_val)  != 0) {
//            LOG(INFO) << "During apply, neighbor value changed from " << old_val << " to "
//                      << old_neighbor_func->GetResultAtomic();
            // revert to normal process
            function->Reset();
            start_ts = 0;
            overlap_cnt_--;
        }
    }

    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.prepare_time += end - start;
        start = end;
    }

    // add element into processed base list
    processed_base_.Put(key, ts, ele);

    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.insert_time += end - start;
        start = end;
    }

    auto sys_ts = ele->id();
    for (auto pid : pids) {
        // LOG(INFO) << "[Joiner " << id() << "]: To Process PID " << pid;
        auto probe_it = peers_.at(pid)->GetProbeTable().NewIterator(key, ticket);
        if (probe_it) {
            probe_it->Seek(ts);
            while (probe_it->Valid()) {
                if (configuration_->time_record) {
                    end = ::baidu::common::timer::get_micros();
                    statistics_.lookup_time += end - start;
                    start = end;
                }

                const Element* other = reinterpret_cast<const Element*>(probe_it->GetValue().data());
                auto curr_ts = other->ts();
                DCHECK_EQ(curr_ts, other->ts());

                if (configuration_->overlapping) {
                    if (curr_ts < start_ts) {
                        break;
                    }
                }

                // HERE is the bug. if Put and Seek happens simultaneously,
                // Seek may return a results whose ts > ts
                // e.g., ts = 6, current skiplist is [9]->[5]
                // FindLessThan will find [9]
                // but before Next() in `Seek()`
                // [7] is inserted after [9]
                // Next() will get node [7] instead of node [5]
                if (!IsInTimeWindow(ts, curr_ts)) {
                    if (curr_ts > ts) {
                        LOG(WARNING) << "Access Probe elements Race happens";
                        probe_it->Next();
                        continue;
                    }
                    DCHECK(curr_ts <= ts);
                    break;
                }
                if (IsVisible(sys_ts, other->id())) {
                    // LOG(INFO) << "[ProcessBaseElement] Apply " << other->ToString() << " on " << ele->ToString();
                    function->AddElementAtomic(other);
                } else {
                    // LOG(INFO) << "[ProcessBaseElement] " << other->ToString() << " Not visible to " <<
                    // ele->ToString();
                }

                if (configuration_->time_record) {
                    end = ::baidu::common::timer::get_micros();
                    statistics_.match_time += end - start;
                    start = end;
                }

                probe_it->Next();
            }
        }
        ticket.Pop();
    }
    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.lookup_time += end - start;
        start = end;
    }

    // output result
    Element output_ele(key, function->GetResultAtomic(), function->GetTs());
    Output(std::move(output_ele));
    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.output_time += end - start;
        statistics_.base_output_time += end - start;
        start = end;
    }
    return 0;
}

int DynamicIntervalJoiner::ProcessProbeElement(Element *ele) {
    DCHECK_EQ(ele->pid(), id());
    DCHECK_NE(ele->hash(), -1);
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

    // add element into processed probe list
    processed_probe_.Put(key, ts, ele);

    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.insert_time += end - start;
        start = end;
    }

    Ticket ticket;
    auto sys_ts = ele->id();
    auto pids = partition_strategy_->GetPartitionSet(ele->hash());
    for (auto pid : pids) {
        // LOG(INFO) << "[Joiner " << id() << "]: To Process PID " << pid;
        auto base_it = peers_.at(pid)->GetBaseTable().NewIterator(key, ticket);
        if (base_it) {
            base_it->Seek(ts + configuration_->window);
            while (base_it->Valid()) {
                if (configuration_->time_record) {
                    end = ::baidu::common::timer::get_micros();
                    statistics_.lookup_time += end - start;
                    start = end;
                }

                const Element* base_ele = reinterpret_cast<const Element*>(base_it->GetValue().data());
                auto curr_ts = base_ele->ts();
                if (!IsInTimeWindow(curr_ts, ts)) {
                    if (curr_ts >= ts) {
                        LOG(WARNING) << "Access Base elements Race happens";
                        base_it->Next();
                        continue;
                    }
                    DCHECK(curr_ts < static_cast<uint64_t>(ts));
                    break;
                }
                ProcessFunction* function = reinterpret_cast<ProcessFunction*>(base_ele->func());
                bool output = false;
                if (IsVisible(sys_ts, base_ele->id())) {
                    DCHECK(function != nullptr);
                    // LOG(INFO) << "[ProcessProbeElement] Apply " << ele->ToString() << " on " << base_ele->ToString();
                    function->AddElementAtomic(ele);
                    output = true;
                } else {
                    if (configuration_->overlapping) {
                        // special handling for overlapping
                        // in ProcessBaseElement, this probe element will be skipped
                        // as its ts < start_ts
                        // we have to remedy here to add the out-of-order probe element
                        // as for the future element as possible as we can
                        // FIXME(zhanghao): there is still a bug case where
                        // upon this pointer, the `base element` is not visible
                        // so there is nothing we can do in the 'ProcessProbe`
                        if (ts < function->start_ts()) {
                            // LOG(INFO) << "Thread race happens";
                            function->AddElementAtomic(ele);
                            output = true;
                        }
                    }
//                    LOG(INFO) << "[ProcessProbeElement] " << base_ele->ToString() << " Not visible to "
//                              << ele->ToString();
                }

                if (configuration_->time_record) {
                    end = ::baidu::common::timer::get_micros();
                    statistics_.match_time += end - start;
                    start = end;
                }

                if (output) {
                    Element output_ele(key, function->GetResultAtomic(), function->GetTs());
                    Output(std::move(output_ele));
                }

                if (configuration_->time_record) {
                    end = ::baidu::common::timer::get_micros();
                    statistics_.output_time += end - start;
                    statistics_.probe_output_time += end - start;
                    start = end;
                }

                base_it->Next();
            }
        }
        ticket.Pop();
    }
    if (configuration_->time_record) {
        end = ::baidu::common::timer::get_micros();
        statistics_.lookup_time += end - start;
        start = end;
    }

    return 0;
}

// TODO(zhanghao): periodically delete the gc_entries_
bool DynamicIntervalJoiner::DoCleanup(size_t curr_ts) {
    int64_t start;
    int64_t end;
    if (configuration_->time_record) {
        start = ::baidu::common::timer::get_micros();
    }
    // cleanup probe
    // for ts == curr_ts - configuration_->window - configuration_->lateness, it should be kept (not cleaned)
    // so we minus 1 as `Expire` will expire all ts <= expire_ts
    int64_t split_ts = curr_ts - configuration_->window - configuration_->lateness - 1;
    if (split_ts >= 0) {
        gc_entries_->push_back(processed_probe_.Expire(split_ts));
    }

    // cleanup base
    // for ts == curr_ts - configuration_->lateness, it should be kept (not cleaned)
    // so we minus 1 as `Expire` will expire all ts <= expire_ts
    split_ts = curr_ts - configuration_->lateness - 1;
    if (split_ts >= 0) {
        gc_entries_->push_back(processed_base_.Expire(split_ts));
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
