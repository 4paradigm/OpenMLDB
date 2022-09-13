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

#include "common/time_statistics.h"

namespace streaming {
namespace interval_join {

std::unique_ptr<TimeStatistics> TimeStatistics::MergeStatistics(const std::vector<TimeStatistics>& stats) {
    size_t size = stats.size();
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
    for (auto& stat : stats) {
        prepare_time += stat.prepare_time;
        lookup_time += stat.lookup_time;
        match_time += stat.match_time;
        output_time += stat.output_time;
        insert_time += stat.insert_time;
        clean_insert_time += stat.clean_insert_time;
        cleaning_time += stat.cleaning_time;
        base_output_time += stat.base_output_time;
        probe_output_time += stat.probe_output_time;
        wait_time += stat.wait_time;
    }

    prepare_time /= size;
    lookup_time /= size;
    match_time /= size;
    output_time /= size;
    insert_time /= size;
    clean_insert_time /= size;
    cleaning_time /= size;
    base_output_time /= size;
    probe_output_time /= size;
    wait_time /= size;
    return std::make_unique<TimeStatistics>(prepare_time, lookup_time, match_time, output_time, insert_time,
                                            clean_insert_time, cleaning_time, base_output_time, probe_output_time,
                                            wait_time);
}

}  // namespace interval_join
}  // namespace streaming
