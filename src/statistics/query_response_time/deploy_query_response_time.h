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

#ifndef SRC_STATISTICS_QUERY_RESPONSE_TIME_DEPLOY_QUERY_RESPONSE_TIME_H_
#define SRC_STATISTICS_QUERY_RESPONSE_TIME_DEPLOY_QUERY_RESPONSE_TIME_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <map>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "statistics/query_response_time/query_response_time.h"

namespace openmldb {
namespace statistics {

struct DeployResponseTimeRow : public ResponseTimeRow {
    DeployResponseTimeRow(const std::string& name, absl::Duration time, uint64_t cnt, absl::Duration total)
        : ResponseTimeRow(time, cnt, total), deploy_name_(name) {}
    ~DeployResponseTimeRow() override {}

    const std::string deploy_name_;
};

inline bool operator==(const DeployResponseTimeRow& lhs, const DeployResponseTimeRow& rhs) {
    return lhs.deploy_name_ == rhs.deploy_name_ && lhs.time_ == rhs.time_ && lhs.total_ == rhs.total_ &&
           lhs.count_ == rhs.count_;
}

// helper class to merge different list of deploy response rows into united one
// that is, after reduce, the output rows will have each row with unique key combine: (deploy_name_ + time)
class DeployResponseTimeRowReducer {
 public:
    using TIME = decltype(DeployResponseTimeRow::time_);
    using COUNT = decltype(DeployResponseTimeRow::count_);
    using TOTAL = decltype(DeployResponseTimeRow::total_);

    void Reduce(const std::string& dp_name, TIME time, COUNT cnt, TOTAL total);

    inline const std::vector<std::shared_ptr<DeployResponseTimeRow>>& Rows() const { return rows_; }

    inline std::shared_ptr<DeployResponseTimeRow> Find(const std::string& dp_name, TIME time) const {
        auto it = cache_.find(dp_name);
        if (it != cache_.end()) {
            auto iit = it->second.find(time);
            if (iit != it->second.end()) {
                return iit->second;
            }
        }
        return {};
    }

 private:
    std::vector<std::shared_ptr<DeployResponseTimeRow>> rows_;
    std::map<std::string, std::map<TIME, std::shared_ptr<DeployResponseTimeRow>>> cache_;
};

class DeployQueryTimeCollector {
 public:
    DeployQueryTimeCollector() {}
    // collector is not copyable
    DeployQueryTimeCollector(const DeployQueryTimeCollector& c) = delete;

    ~DeployQueryTimeCollector() {}

    absl::Status Collect(const std::string& deploy_name, absl::Duration time) LOCKS_EXCLUDED(mutex_);

    absl::Status AddDeploy(const std::string& deploy_name) LOCKS_EXCLUDED(mutex_);

    absl::Status DeleteDeploy(const std::string& deploy_name) LOCKS_EXCLUDED(mutex_);

    std::vector<DeployResponseTimeRow> Flush() LOCKS_EXCLUDED(mutex_);

    absl::StatusOr<std::vector<DeployResponseTimeRow>> GetRows(const std::string& deploy_name) const
        LOCKS_EXCLUDED(mutex_);

    std::vector<DeployResponseTimeRow> GetRows() const LOCKS_EXCLUDED(mutex_);

 private:
    uint32_t GetRecordsCnt() const SHARED_LOCKS_REQUIRED(mutex_);

 private:
    std::unordered_map<std::string, std::shared_ptr<TimeCollector>> collectors_ GUARDED_BY(mutex_);
    mutable absl::Mutex mutex_;  // protects collectors_
};

}  // namespace statistics
}  // namespace openmldb

#endif  // SRC_STATISTICS_QUERY_RESPONSE_TIME_DEPLOY_QUERY_RESPONSE_TIME_H_
