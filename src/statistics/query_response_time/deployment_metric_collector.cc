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

#include "statistics/query_response_time/deployment_metric_collector.h"

namespace openmldb::statistics {

absl::Status DeploymentMetricCollector::Collect(const std::string& db, const std::string& deploy_name,
                                                absl::Duration time) {
    absl::ReaderMutexLock lock(&mutex_);
    auto it = md_recorder_->get_stats({db, deploy_name});
    if (it == nullptr) {
        LOG(WARNING) << "reach limit size, collect failed";
        return absl::OutOfRangeError("multi-dimensional recorder reaches limit size, please delete old deploy");
    }
    *it << absl::ToInt64Microseconds(time);
    return absl::OkStatus();
}

absl::Status DeploymentMetricCollector::DeleteDeploy(const std::string& db, const std::string& deploy_name) {
    absl::ReaderMutexLock lock(&mutex_);
    md_recorder_->delete_stats({db, deploy_name});
    return absl::OkStatus();
}

void DeploymentMetricCollector::Reset() {
    absl::WriterMutexLock lock(&mutex_);
    md_recorder_ = make_shared(prefix_);
}
}  // namespace openmldb::statistics
