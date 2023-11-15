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

#ifndef HYBRIDSE_SRC_VM_RUNNER_CTX_H_
#define HYBRIDSE_SRC_VM_RUNNER_CTX_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "vm/cluster_task.h"

namespace hybridse {
namespace vm {

class RunnerContext {
 public:
    explicit RunnerContext(hybridse::vm::ClusterJob* cluster_job,
                           const hybridse::codec::Row& parameter,
                           const bool is_debug = false)
        : cluster_job_(cluster_job),
          sp_name_(""),
          request_(),
          requests_(),
          parameter_(parameter),
          is_debug_(is_debug),
          batch_cache_() {}
    explicit RunnerContext(hybridse::vm::ClusterJob* cluster_job,
                           const hybridse::codec::Row& request,
                           const std::string& sp_name = "",
                           const bool is_debug = false)
        : cluster_job_(cluster_job),
          sp_name_(sp_name),
          request_(request),
          requests_(),
          parameter_(),
          is_debug_(is_debug),
          batch_cache_() {}
    explicit RunnerContext(hybridse::vm::ClusterJob* cluster_job,
                           const std::vector<Row>& request_batch,
                           const std::string& sp_name = "",
                           const bool is_debug = false)
        : cluster_job_(cluster_job),
          sp_name_(sp_name),
          request_(),
          requests_(request_batch),
          parameter_(),
          is_debug_(is_debug),
          batch_cache_() {}

    const size_t GetRequestSize() const { return requests_.size(); }
    const hybridse::codec::Row& GetRequest() const { return request_; }
    const hybridse::codec::Row& GetRequest(size_t idx) const {
        return requests_[idx];
    }
    const hybridse::codec::Row& GetParameterRow() const { return parameter_; }
    hybridse::vm::ClusterJob* cluster_job() { return cluster_job_; }
    void SetRequest(const hybridse::codec::Row& request);
    void SetRequests(const std::vector<hybridse::codec::Row>& requests);
    bool is_debug() const { return is_debug_; }

    const std::string& sp_name() { return sp_name_; }
    std::shared_ptr<DataHandler> GetCache(int64_t id) const;
    void SetCache(int64_t id, std::shared_ptr<DataHandler> data);
    void ClearCache() { cache_.clear(); }
    std::shared_ptr<DataHandlerList> GetBatchCache(int64_t id) const;
    void SetBatchCache(int64_t id, std::shared_ptr<DataHandlerList> data);

 private:
    hybridse::vm::ClusterJob* cluster_job_;
    const std::string sp_name_;
    hybridse::codec::Row request_;
    std::vector<hybridse::codec::Row> requests_;
    hybridse::codec::Row parameter_;
    size_t idx_;
    const bool is_debug_;
    // TODO(chenjing): optimize
    std::map<int64_t, std::shared_ptr<DataHandler>> cache_;
    std::map<int64_t, std::shared_ptr<DataHandlerList>> batch_cache_;
};

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_RUNNER_CTX_H_
