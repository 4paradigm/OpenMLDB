/**
 * Copyright (c) 2023 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "vm/runner_ctx.h"

namespace hybridse {
namespace vm {

std::shared_ptr<DataHandlerList> RunnerContext::GetBatchCache(int64_t id) const {
    auto iter = batch_cache_.find(id);
    if (iter == batch_cache_.end()) {
        return std::shared_ptr<DataHandlerList>();
    } else {
        return iter->second;
    }
}

void RunnerContext::SetBatchCache(int64_t id, std::shared_ptr<DataHandlerList> data) { batch_cache_[id] = data; }

std::shared_ptr<DataHandler> RunnerContext::GetCache(int64_t id) const {
    auto iter = cache_.find(id);
    if (iter == cache_.end()) {
        return std::shared_ptr<DataHandler>();
    } else {
        return iter->second;
    }
}

void RunnerContext::SetCache(int64_t id, const std::shared_ptr<DataHandler> data) { cache_[id] = data; }

void RunnerContext::SetRequest(const hybridse::codec::Row& request) { request_ = request; }
void RunnerContext::SetRequests(const std::vector<hybridse::codec::Row>& requests) { requests_ = requests; }

}  // namespace vm
}  // namespace hybridse
