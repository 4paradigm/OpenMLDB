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
#ifndef STREAM_SRC_EXECUTOR_PARTITION_STRATEGY_H_
#define STREAM_SRC_EXECUTOR_PARTITION_STRATEGY_H_

#include <glog/logging.h>
#include <gtest/gtest_prod.h>
#include <gflags/gflags.h>
#include <math.h>

#include <atomic>
#include <map>
#include <memory>
#include <queue>
#include <utility>
#include <string>
#include <unordered_map>
#include <vector>

namespace streaming {
namespace interval_join {

enum class PartitionType {
    kHash,
};

class PartitionStrategy {
 public:
    virtual ~PartitionStrategy() {}
    virtual int GetPartitionId(const std::string& key, int* hash) = 0;
    int GetPartitionId(const std::string& key) {
        int hash;
        return GetPartitionId(key, &hash);
    }
    virtual int UpdateSchedule() = 0;
    virtual std::vector<int> GetPartitionSet(int hash) = 0;
    virtual std::vector<int> GetPartitionSet(const std::string& key, int* hash) = 0;
    virtual int64_t GenId(int hash) = 0;
};

class HashPartitionStrategy : public PartitionStrategy {
 public:
    FRIEND_TEST(PartitionerTest, HashStrategyTest);
    explicit HashPartitionStrategy(int slot_num) : slot_num_(slot_num), ids_(slot_num) {
        for (int i = 0; i < slot_num; i++) {
            ids_[i] = 0;
        }
    }

    using PartitionStrategy::GetPartitionId;
    int GetPartitionId(const std::string& key, int* hash) override {
        auto slot = std::hash<std::string>()(key) % slot_num_;
        *hash = slot;
        return slot;
    }
    int UpdateSchedule() override { return 0; }

    std::vector<int> GetPartitionSet(int hash) override {
        return {hash};
    }

    inline int64_t GenId(int hash) override {
        return ids_[hash]++;
    }

    std::vector<int> GetPartitionSet(const std::string& key, int* hash) override { return {GetPartitionId(key, hash)}; }

 private:
    int slot_num_ = 0;
    std::vector<std::atomic<int64_t>> ids_;
};

// [start, end]
struct Range {
    Range(int start, int end) : start(start), end(end) {}
    Range(const Range& other) : start(other.start), end(other.end) {}

    int start;
    int end;
};

class Assignment {
 public:
    Assignment() {}
    virtual ~Assignment() {}

    virtual bool IsAssigned(int hash) = 0;
    virtual int Assign(int hash) = 0;
    virtual int Allocated(int hash) = 0;

    virtual int Assign(const Range& range) {
        for (int i = range.start; i <= range.end; i++) {
            auto ret = Assign(i);
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }

    virtual int Assign(const std::vector<int>& assigns) {
        for (int i : assigns) {
            auto ret = Assign(i);
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }
};

class SlotAssignment : public Assignment {
 public:
    SlotAssignment() {}
    virtual ~SlotAssignment() {}

    bool IsAssigned(int hash) override { return assigned_.count(hash); }

    int Assign(int hash) override {
        auto it = assigned_.find(hash);
        if (it == assigned_.end()) {
            assigned_[hash] = 0;
        }

        return 0;
    }

    using Assignment::Assign;

    int Allocated(int hash) override {
        assigned_[hash]++;
        score_++;
        return 0;
    }

 private:
    // hash -> score
    std::unordered_map<int, std::atomic<int64_t>> assigned_;
    std::atomic<int64_t> score_ = 0;
};

class RangeAssignment : public Assignment {
 public:
    FRIEND_TEST(PartitionerTest, RangeAssignmentTest);
    bool IsAssigned(int hash) override;

    int Assign(const Range& range) override;
    int Assign(int hash) override {
        Range r(hash, hash);
        return Assign(r);
    }

    int Allocated(int hash) override {
        LOG(ERROR) << "Not support allocated for RangeAssignment";
        return -1;
    }

 private:
    std::map<int, Range> ranges_;
};

class BalancedPartitionStrategy : public PartitionStrategy {
 public:
    FRIEND_TEST(PartitionerTest, BalancedStrategyTest);
    class TimeElapseRandom {
     public:
        explicit TimeElapseRandom(int size, int step = 1) : size_(size), step_(step) {}

        int Random() {
            if (curr_cnt_ >= step_) {
                curr_ = (curr_ + 1) % size_;
                curr_cnt_ = 0;
            }
            curr_cnt_++;
            return curr_;
        }

     private:
        const int size_;
        const int step_;
        std::atomic<int> curr_ = 0;
        std::atomic<int> curr_cnt_ = 0;
    };

    using Hash2Partition = std::vector<std::vector<int>>;
    using Hash2Random = std::vector<std::unique_ptr<TimeElapseRandom>>;

    enum InitType {
        kHash,
        kRange,
    };

    // hash_size shouldn't be too large. it may affect the performance
    explicit BalancedPartitionStrategy(int slot_num, int hash_size = 1024, InitType init_type = kHash);

    int GetPartitionId(const std::string& key, int* hash) override;

    int UpdateSchedule() override;

    void UpdateAssignment(const std::shared_ptr<std::vector<std::shared_ptr<Assignment>>>& assignments) {
        auto hash_to_partition = GenHashPartition(*assignments);
        auto hash_to_random = GenRandom(hash_to_partition);
        std::atomic_store_explicit(&assignments_, assignments, std::memory_order_relaxed);
        std::atomic_store_explicit(&hash_to_partition_, hash_to_partition, std::memory_order_relaxed);
        std::atomic_store_explicit(&hash_to_random_, hash_to_random, std::memory_order_relaxed);
    }

    // this will be called in joiner to check all the corresponding joiner threads
    std::vector<int> GetPartitionSet(int hash) override {
        std::shared_ptr<Hash2Partition> hash_to_partition =
            atomic_load_explicit(&hash_to_partition_, std::memory_order_relaxed);
        return hash_to_partition->at(hash);
    }

    std::vector<int> GetPartitionSet(const std::string& key, int* hash) override {
        auto h = std::hash<std::string>()(key) % hash_size_;
        auto stats = atomic_load_explicit(&stats_, std::memory_order_relaxed);
        atomic_fetch_add_explicit(&stats->at(h), 1L, std::memory_order_relaxed);
        *hash = h;
        return GetPartitionSet(*hash);
    }

    inline int64_t GenId(int hash) override {
        return ids_[hash]++;
    }

    template <class T>
    static double Stddev(const std::vector<T>& scores, T target_score) {
        double res = 0;
        for (int i = 0; i < scores.size(); i++) {
            res += std::pow(scores[i] - target_score, 2);
        }
        res /= scores.size();
        return std::sqrt(res) / target_score;
    }

 private:
    std::shared_ptr<Hash2Partition> GenHashPartition(
        const std::vector<std::shared_ptr<Assignment>>& assignments) const;

    std::shared_ptr<Hash2Random> GenRandom(const std::shared_ptr<Hash2Partition>& hash_2_partition) const;

    double Stddev(const std::vector<std::pair<int, double>>& scores, double target_score) {
        std::vector<double> score_vals(scores.size());
        for (size_t i = 0; i < scores.size(); i++) {
            score_vals[i] = scores[i].second;
        }
        return Stddev(score_vals, target_score);
    }

    const int slot_num_;
    const int hash_size_;
    std::shared_ptr<std::vector<std::shared_ptr<Assignment>>> assignments_;
    std::shared_ptr<Hash2Partition> hash_to_partition_;
    std::shared_ptr<Hash2Random> hash_to_random_;
    std::shared_ptr<std::vector<std::atomic<int64_t>>> stats_;
    unsigned seed = 0;
    std::vector<std::atomic<int64_t>> ids_;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_EXECUTOR_PARTITION_STRATEGY_H_
