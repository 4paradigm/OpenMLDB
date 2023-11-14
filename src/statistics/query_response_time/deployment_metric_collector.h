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
#ifndef SRC_STATISTICS_QUERY_RESPONSE_TIME_DEPLOYMENT_METRIC_COLLECTOR_H_
#define SRC_STATISTICS_QUERY_RESPONSE_TIME_DEPLOYMENT_METRIC_COLLECTOR_H_

#include <list>
#include <memory>
#include <sstream>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "bvar/bvar.h"
#include "bvar/multi_dimension.h"
#include "gflags/gflags.h"

namespace openmldb::statistics {
class DeploymentMetricCollector {
 public:
    typedef typename bvar::MultiDimension<bvar::LatencyRecorder> MDRecorder;
    explicit DeploymentMetricCollector(const std::string& prefix) : prefix_(prefix), md_recorder_(make_shared(prefix)) {
        // already expose_as when MultiDimension ctor
    }
    // collector is not copyable
    DeploymentMetricCollector(const DeploymentMetricCollector& c) = delete;

    ~DeploymentMetricCollector() {}
    // <db>.<deploy_name>
    absl::Status Collect(const std::string& db, const std::string& deploy_name, absl::Duration time)
        LOCKS_EXCLUDED(mutex_);
    absl::Status DeleteDeploy(const std::string& db, const std::string& deploy_name) LOCKS_EXCLUDED(mutex_);
    void Reset() LOCKS_EXCLUDED(mutex_);

    // usually used for debug
    std::string Desc(const std::list<std::string>& key) LOCKS_EXCLUDED(mutex_) {
        absl::ReaderMutexLock lock(&mutex_);
        std::stringstream ss;
        if (key.empty()) {
            md_recorder_->describe(ss);
        } else if (md_recorder_->has_stats(key)) {
            auto rd = md_recorder_->get_stats(key);
            ss << "count:" << rd->count() << ", qps:" << rd->qps() << ", latency:[" << rd->latency() << ","
               << rd->latency_percentile(0.8) << "," << rd->latency_percentile(0.9) << ","
               << rd->latency_percentile(0.99) << "," << rd->latency_percentile(0.999) << ","
               << rd->latency_percentile(0.9999) << "]";
        } else {
            ss << "no stats for key";
        }

        return ss.str();
    }

    static std::shared_ptr<MDRecorder> make_shared(const std::string& prefix) {
        MDRecorder::key_type labels = {"db", "deployment"};
        return std::make_shared<MDRecorder>(prefix, "deployment", labels);
    }

 private:
    std::string prefix_;  // for reset
    // not copyable and can't clear, so use ptr
    // MultiDimension can't define recorder window size by yourself, bvar_dump_interval is the only way
    std::shared_ptr<MDRecorder> md_recorder_ GUARDED_BY(mutex_);
    mutable absl::Mutex mutex_;  // protects collectors_
};
}  // namespace openmldb::statistics
#endif  // SRC_STATISTICS_QUERY_RESPONSE_TIME_DEPLOYMENT_METRIC_COLLECTOR_H_
