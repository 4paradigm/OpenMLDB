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

#include "executor/partition_strategy.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <algorithm>
#include <limits>
#include <utility>
#include <set>

namespace streaming {
namespace interval_join {

DEFINE_double(score_min_threshold, 0.1,
              "the threshold over which the score change is considered significant for partition re-organization");
DEFINE_double(stddev_min_threshold, 0.01,
              "the stddev threshold over which schedule exploration will continue");

DEFINE_int32(time_random_step, 100, "elapse step for TimeElapseRandom");
DEFINE_bool(time_random, false, "whether use TimeElapseRandom");
DEFINE_double(stat_damping_factor, 0.9, "the stat damping factor");

bool RangeAssignment::IsAssigned(int hash) {
    auto it = ranges_.lower_bound(hash);
    if (it == ranges_.end()) {
        return false;
    }

    return hash >= it->second.start;
}

int RangeAssignment::Assign(const Range& r) {
    Range range = r;
    auto it = ranges_.lower_bound(range.start);
    std::vector<int> to_remove;
    auto start = it;
    auto end = ranges_.begin();
    while (it != ranges_.end()) {
        if (it->second.start > range.end) {
            break;
        }

        range.start = std::min(range.start, it->second.start);
        range.end = std::max(range.end, it->second.end);
        it++;
        end = it;
    }

    if (end != ranges_.begin()) {
        ranges_.erase(start, end);
    }

    ranges_.emplace(std::make_pair(range.end, range));
    return 0;
}

BalancedPartitionStrategy::BalancedPartitionStrategy(int slot_num, int hash_size, InitType init_type)
    : slot_num_(slot_num),
      assignments_(std::make_shared<std::vector<std::shared_ptr<Assignment>>>(slot_num)),
      hash_to_partition_(std::make_shared<Hash2Partition>()),
      hash_to_random_(std::make_shared<Hash2Random>()),
      stats_(std::make_shared<std::vector<std::atomic<int64_t>>>(hash_size)),
      hash_size_(hash_size / slot_num * slot_num),
      ids_(hash_size) {
    int step = hash_size / slot_num;
    for (int i = 0; i < slot_num; i++) {
        assignments_->at(i) = std::make_shared<SlotAssignment>();

        if (init_type == kRange) {
            assignments_->at(i)->Assign(Range(i * step, (i + 1) * step - 1));
        }
    }

    for (int i = 0; i < hash_size; i++) {
        if (init_type == kHash) {
            assignments_->at(i % slot_num)->Assign(i);
        }
        stats_->at(i) = 0;
    }

    auto hash_to_partition = GenHashPartition(*assignments_);
    auto hash_to_randoms = GenRandom(hash_to_partition);
    std::atomic_store_explicit(&hash_to_partition_, hash_to_partition, std::memory_order_relaxed);
    std::atomic_store_explicit(&hash_to_random_, hash_to_randoms, std::memory_order_relaxed);
}

int BalancedPartitionStrategy::GetPartitionId(const std::string& key, int* hash) {
    auto partition_set = GetPartitionSet(key, hash);
    int rand_idx = 0;
    if (partition_set.size() > 1) {
        if (FLAGS_time_random) {
            DLOG(INFO) << "Use time elapsed random";
            auto hash_to_random = atomic_load_explicit(&hash_to_random_, std::memory_order_relaxed);
            rand_idx = hash_to_random->at(*hash)->Random();
        } else {
            rand_idx = rand_r(&seed) % partition_set.size();
        }
    }
    return partition_set.at(rand_idx);
}

int BalancedPartitionStrategy::UpdateSchedule() {
    // total scores for all joiners
    std::vector<std::pair<int, double>> scores(slot_num_);
    // hash -> pid set map
    std::vector<std::vector<int>> hash_2_pids(hash_size_);
    // pid -> hash set map
    std::vector<std::set<int>> pid_2_hashes(slot_num_);

    double total_score = 0;
    for (int i = 0; i < slot_num_; i++) {
        scores[i] = std::make_pair(i, 0);
    }
    std::shared_ptr<Hash2Partition> hash_to_partition =
        atomic_load_explicit(&hash_to_partition_, std::memory_order_relaxed);
    auto orig_stats = atomic_load_explicit(&stats_, std::memory_order_relaxed);
    std::vector<std::atomic<int64_t>> stats(orig_stats->size());
    for (int i = 0; i < orig_stats->size(); i++) {
        stats[i] = orig_stats->at(i).load();
    }

    for (int i = 0; i < hash_size_; i++) {
        auto ass_set = hash_to_partition->at(i);
        for (int j : ass_set) {
            scores[j].second += static_cast<double>(stats.at(i).load(std::memory_order_relaxed)) / ass_set.size();
            pid_2_hashes[j].insert(i);
        }
        hash_2_pids[i] = ass_set;
        total_score += stats.at(i);
    }
    const double target_score = total_score / slot_num_;
    auto compare = [](const std::pair<int, double>& a, const std::pair<int, double>& b) {
        return a.second < b.second;
    };

    class HashCompare {
     public:
        bool operator() (const std::pair<int, double>& hash_score_a, const std::pair<int, double>& hash_score_b) {
            return hash_score_a.second < hash_score_b.second;
        }
    };

    double last_dev = Stddev(scores, target_score);
    double new_dev = last_dev;
    double old_dev = last_dev;
    bool updated;
    bool changed = false;
    do {
        updated = false;
        // find the pid with max score
        auto it = std::max_element(scores.begin(), scores.end(), compare);
        int max_pid = it->first;
        double max_score = it->second;
        if (max_score <= target_score) {
            break;
        }

        // add to priority queue and loop all top elements
        // the score is "release score", which means, if we duplicate this hash to another pid
        // how much score will be changed
        std::priority_queue<std::pair<int, double>, std::vector<std::pair<int, double>>, HashCompare> hashes_q;
        for (auto hash : pid_2_hashes.at(max_pid)) {
            int pid_size = hash_2_pids.at(hash).size();
            double score = stats.at(hash).load();
            if (score <= 0) continue;
            hashes_q.push(std::make_pair(hash, score / pid_size - score / (pid_size + 1)));
        }
        while (!hashes_q.empty()) {
            // find the hash with max release score and |hash set| < #pids
            std::pair<int, double> to_dup = hashes_q.top();
            hashes_q.pop();
            int max_hash = to_dup.first;
            // if the hash is already allocated to all slots
            // or its score - release score < target score
            if (hash_2_pids.at(max_hash).size() >= slot_num_) {
                continue;
            }

            auto& pid_set = hash_2_pids.at(max_hash);
            double to_release_score = to_dup.second;
            if (to_release_score < FLAGS_score_min_threshold) {
                break;
            }

            // find the pid with min score && not cover max_hash && score + to_release_score < target_score
            double score = std::numeric_limits<double>::max();
            int min_pid = -1;
            for (int i = 0; i < slot_num_; i++) {
                DCHECK_EQ(i, scores.at(i).first);
                if (i == max_pid) continue;

                auto& curr_s = scores.at(i);
                if (curr_s.second < score && !pid_2_hashes.at(i).count(max_hash)) {
                    score = curr_s.second;
                    min_pid = curr_s.first;
                }
            }
            if (min_pid < 0) continue;

            std::vector<std::pair<int, double>> new_scores = scores;
            // update all the scores covering max_hash
            for (auto& score : new_scores) {
                int pid = score.first;
                if (pid_2_hashes.at(pid).count(max_hash)) {
                    CHECK_NE(pid, min_pid);
                    new_scores.at(pid).second -= to_release_score;
                }
            }

            new_scores.at(min_pid).second += to_release_score;

            double dev = Stddev(new_scores, target_score);
            if (dev >= last_dev || last_dev - dev <= FLAGS_stddev_min_threshold) {
                updated = false;
                continue;
            } else {
                last_dev = dev;
            }

            scores = new_scores;
            hash_2_pids.at(max_hash).push_back(min_pid);
            pid_2_hashes.at(min_pid).insert(max_hash);
            new_dev = dev;
            updated = true;
            changed = true;
            break;
        }
    } while (updated);

    // update the assignment
    if (changed) {
        std::shared_ptr<std::vector<std::shared_ptr<Assignment>>> assignments =
            std::make_shared<std::vector<std::shared_ptr<Assignment>>>(slot_num_);
        for (int i = 0; i < slot_num_; i++) {
            assignments->at(i) = std::make_shared<SlotAssignment>();
            for (auto hash : pid_2_hashes[i]) {
                assignments->at(i)->Assign(hash);
            }
        }
        UpdateAssignment(assignments);
        LOG(INFO) << "[Scheduler] Updated work assignment: stddev update from " << old_dev << " to " << new_dev;
    } else {
        DLOG(INFO) << "[Scheduler] No need to update work assignment";
    }

    // reset the stats
    auto new_stats = std::make_shared<std::vector<std::atomic<int64_t>>>(hash_size_);
    auto old_stats = std::atomic_load_explicit(&stats_, std::memory_order_relaxed);
    for (int i = 0; i < hash_size_; i++) {
        new_stats->at(i) = old_stats->at(i) * FLAGS_stat_damping_factor;
    }
    atomic_store_explicit(&stats_, new_stats, std::memory_order_relaxed);
    if (changed) {
        return 0;
    } else {
        return 1;
    }
}

std::shared_ptr<BalancedPartitionStrategy::Hash2Partition> BalancedPartitionStrategy::GenHashPartition(
    const std::vector<std::shared_ptr<Assignment>>& assignments) const {
    std::shared_ptr<Hash2Partition> hash_to_partition = std::make_shared<Hash2Partition>(hash_size_);
    CHECK(assignments.size() == slot_num_);
    for (int i = 0; i < hash_size_; i++) {
        for (int j = 0; j < slot_num_; j++) {
            if (assignments.at(j)->IsAssigned(i)) {
                auto& h2p = (*hash_to_partition)[i];
                h2p.push_back(j);
            }
        }
    }

    return hash_to_partition;
}

std::shared_ptr<BalancedPartitionStrategy::Hash2Random> BalancedPartitionStrategy::GenRandom(
    const std::shared_ptr<Hash2Partition>& hash_2_partition) const {
    std::shared_ptr<Hash2Random> randoms = std::make_shared<Hash2Random>(hash_2_partition->size());
    CHECK_EQ(hash_2_partition->size(), hash_size_);
    for (int i = 0; i < hash_size_; i++) {
        randoms->at(i) = std::make_unique<TimeElapseRandom>(hash_2_partition->at(i).size(), FLAGS_time_random_step);
    }

    return randoms;
}

}  // namespace interval_join
}  // namespace streaming
