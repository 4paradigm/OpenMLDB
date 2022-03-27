/*
 * Copyright 2022 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "statistics/query_response_time/deploy_query_response_time.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "glog/logging.h"

namespace openmldb {
namespace statistics {

absl::Status DeployQueryTimeCollector::Collect(const std::string& deploy_name, absl::Duration time) {
    absl::ReaderMutexLock lock(&mutex_);
    auto it = collectors_.find(deploy_name);
    if (it == collectors_.end()) {
        return absl::NotFoundError(absl::StrCat("deploy name ", deploy_name, " not found"));
    }

    it->second->Collect(time);
    return absl::OkStatus();
}

absl::Status DeployQueryTimeCollector::AddDeploy(const std::string& deploy_name) {
    absl::WriterMutexLock lock(&mutex_);
    if (collectors_.find(deploy_name) != collectors_.end()) {
        return absl::AlreadyExistsError(absl::StrCat("deploy name ", deploy_name, " already exists"));
    }
    collectors_.emplace(deploy_name, std::make_shared<TimeCollector>());
    return absl::OkStatus();
}

absl::Status DeployQueryTimeCollector::DeleteDeploy(const std::string& deploy_name) {
    absl::WriterMutexLock lock(&mutex_);
    auto it = collectors_.find(deploy_name);
    if (it == collectors_.end()) {
        return absl::NotFoundError(absl::StrCat("deploy name ", deploy_name, " not found"));
    }

    collectors_.erase(it);
    return absl::OkStatus();
}

absl::StatusOr<DeployResponseTimeRow> DeployQueryTimeCollector::GetRow(const std::string& deploy_name,
                                                                       size_t idx) const {
    absl::ReaderMutexLock lock(&mutex_);
    return GetRowUnsafe(deploy_name, idx);
}

absl::StatusOr<std::vector<DeployResponseTimeRow>> DeployQueryTimeCollector::GetRows(const std::string& deploy_name) {
    absl::ReaderMutexLock lock(&mutex_);
    auto it = collectors_.find(deploy_name);
    if (it == collectors_.end()) {
        return absl::NotFoundError(absl::StrCat("deploy name ", deploy_name, " not found"));
    }

    std::vector<DeployResponseTimeRow> rows;
    rows.reserve(it->second->BucketCount());
    for (auto idx = 0; idx < it->second->BucketCount(); ++idx) {
        auto row = it->second->GetRow(idx);
        rows.emplace_back(it->first, row->upper_bound_, row->count_, row->total_);
    }
    return rows;
}

std::vector<DeployResponseTimeRow> DeployQueryTimeCollector::GetRows() const {
    absl::ReaderMutexLock lock(&mutex_);
    std::vector<DeployResponseTimeRow> rows;
    rows.reserve(GetRecordsCnt());
    for (auto& kv : collectors_) {
        for (auto idx = 0; idx < kv.second->BucketCount(); ++idx) {
            auto row = kv.second->GetRow(idx);
            rows.emplace_back(kv.first, row->upper_bound_, row->count_, row->total_);
        }
    }
    return rows;
}

std::vector<DeployResponseTimeRow> DeployQueryTimeCollector::Flush() {
    absl::ReaderMutexLock lock(&mutex_);

    std::vector<DeployResponseTimeRow> rows;
    rows.reserve(GetRecordsCnt());
    for (auto& kv : collectors_) {
        for (auto idx = 0; idx < kv.second->BucketCount(); ++idx) {
            auto row = kv.second->Flush(idx);
            rows.emplace_back(kv.first, row->upper_bound_, row->count_, row->total_);
        }
    }

    return rows;
}


absl::StatusOr<DeployResponseTimeRow> DeployQueryTimeCollector::GetRowUnsafe(const std::string& deploy_name,
                                                                       size_t idx) const {
    auto it = collectors_.find(deploy_name);
    if (it == collectors_.end()) {
        return absl::NotFoundError(absl::StrCat("deploy name ", deploy_name, " not found"));
    }

    auto row = it->second->GetRow(idx);
    if (!row.ok()) {
        return row.status();
    }
    return DeployResponseTimeRow{deploy_name, row->upper_bound_, row->count_, row->total_};
}

// thread unsafe
uint32_t DeployQueryTimeCollector::GetRecordsCnt() const {
    uint32_t cnt = 0;
    for (auto& kv : collectors_) {
        cnt += kv.second->BucketCount();
    }
    return cnt;
}

}  // namespace statistics
}  // namespace openmldb
