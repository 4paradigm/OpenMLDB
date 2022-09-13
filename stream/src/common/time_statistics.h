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
#include <gflags/gflags.h>
#include <vector>
#include <memory>

#ifndef STREAM_SRC_COMMON_TIME_STATISTICS_H_
#define STREAM_SRC_COMMON_TIME_STATISTICS_H_

namespace streaming {
namespace interval_join {

struct TimeStatistics {
    TimeStatistics(int64_t prepare_time = 0, int64_t lookup_time = 0, int64_t match_time = 0, int64_t  output_time = 0,
                   int64_t insert_time = 0, int64_t clean_insert_time = 0, int64_t cleaning_time = 0,
                   int64_t base_output_time = 0, int64_t probe_output_time = 0, int64_t wait_time = 0)
        : prepare_time(prepare_time),
          lookup_time(lookup_time),
          match_time(match_time),
          output_time(output_time),
          insert_time(insert_time),
          clean_insert_time(clean_insert_time),
          cleaning_time(cleaning_time),
          base_output_time(base_output_time),
          probe_output_time(probe_output_time),
          wait_time(wait_time) {}

    int64_t prepare_time = 0;
    int64_t lookup_time = 0;
    int64_t match_time = 0;
    int64_t output_time = 0;
    int64_t insert_time = 0;
    int64_t clean_insert_time = 0;
    int64_t cleaning_time = 0;
    int64_t base_output_time = 0;
    int64_t probe_output_time = 0;
    int64_t wait_time = 0;

    void Print(bool csv = true) {
        int64_t total = prepare_time + lookup_time + match_time + output_time + insert_time + clean_insert_time +
                        cleaning_time + wait_time;
        if (csv) {
            LOG(INFO) << "the time breakdown (prepare, lookup, match, output, insert, clean_insert, cleaning, wait, "
                         "total) is: "
                      << prepare_time << ", " << lookup_time << ", " << match_time << ", " << output_time << ", "
                      << insert_time << ", " << clean_insert_time << ", " << cleaning_time << ", " << wait_time << ", "
                      << total;
        }
        LOG(INFO) << "prepare: " << prepare_time
                  << ", lookup:" << lookup_time
                  << ", match:" << match_time
                  << ", output:" << output_time
                  << ", base_output:" << base_output_time
                  << ", probe_output:" << probe_output_time
                  << ", insert:" << insert_time
                  << ", clean_insert:" << clean_insert_time
                  << ", cleaning: " << cleaning_time
                  << ", wait: " << wait_time
                  << ", stat time: " << (total/1000)
                  << " ms" << ", effective time: "
                  << (total - wait_time) / 1000 << " ms";
    }

    static std::unique_ptr<TimeStatistics> MergeStatistics(const std::vector<TimeStatistics>& stats);
};

}  // namespace interval_join
}  // namespace streaming
#endif  // STREAM_SRC_COMMON_TIME_STATISTICS_H_
