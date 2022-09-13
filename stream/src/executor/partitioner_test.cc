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

#include "executor/partitioner.h"

#include <absl/strings/str_cat.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "common/ring_buffer.h"
#include "stream/simple_data_stream.h"

namespace streaming {
namespace interval_join {

DECLARE_double(stat_damping_factor);

class PartitionerTest : public ::testing::Test {};

TEST_F(PartitionerTest, SmokeTest) {
    SimpleDataStream stream(100, 100);
    HashPartitionStrategy strategy(2);
    Partitioner partitioner(&stream, &strategy);
    auto buffer = std::make_shared<RingBuffer>(100);
    partitioner.AddBuffer(buffer);
    partitioner.AddBuffer(buffer);
    partitioner.Run();
    partitioner.Join();
    EXPECT_EQ(100, partitioner.GetProcessedCounter());
    int count0 = 0, count1 = 0;
    while (auto ele = buffer->GetUnblocking()) {
        if (ele->pid() == 0) {
            count0++;
        } else if (ele->pid() == 1) {
            count1++;
        } else {
            ASSERT_TRUE(false);
        }
    }
    ASSERT_NE(0, count0);
    ASSERT_NE(0, count1);
    ASSERT_EQ(100, count0 + count1);
}

TEST_F(PartitionerTest, MultiplePartitionersTest) {
    SimpleDataStream stream(1000, 100);
    HashPartitionStrategy strategy(2);
    Partitioner partitioner0(&stream, &strategy);
    Partitioner partitioner1(&stream, &strategy);
    auto buffer = std::make_shared<RingBuffer>(1000);
    partitioner0.AddBuffer(buffer);
    partitioner0.AddBuffer(buffer);

    partitioner1.AddBuffer(buffer);
    partitioner1.AddBuffer(buffer);

    partitioner0.Run();
    partitioner1.Run();

    partitioner0.Join();
    partitioner1.Join();
    EXPECT_EQ(1000, partitioner0.GetProcessedCounter() + partitioner1.GetProcessedCounter());

    int count0 = 0, count1 = 0;
    while (auto ele = buffer->GetUnblocking()) {
        if (ele->pid() == 0) {
            count0++;
        } else if (ele->pid() == 1) {
            count1++;
        } else {
            ASSERT_TRUE(false);
        }
    }
    ASSERT_EQ(1000, count0 + count1);
    ASSERT_NE(0, count0);
    ASSERT_NE(0, count1);
}

TEST_F(PartitionerTest, HashStrategyTest) {
    int slot_num = 4;
    HashPartitionStrategy partition_strategy(slot_num);
    int stats[slot_num];  // NOLINT
    for (int i = 0; i < slot_num; i++) {
        stats[i] = 0;
    }

    int key_num = 10000;
    for (int i = 0; i < key_num; i++) {
        std::string key = absl::StrCat("key_" + std::to_string(i));
        int pid = partition_strategy.GetPartitionId(key);
        int hash;
        auto pid_set = partition_strategy.GetPartitionSet(key, &hash);
        ASSERT_EQ(1, pid_set.size());
        ASSERT_EQ(pid, pid_set.at(0));
        ASSERT_EQ(std::hash<std::string>()(key) % slot_num, hash);
        auto pid_set2 = partition_strategy.GetPartitionSet(hash);
        ASSERT_EQ(pid_set, pid_set2);

        EXPECT_TRUE(pid >= 0 && pid < slot_num);
        stats[pid]++;
    }
    for (int i = 0; i < slot_num; i++) {
        EXPECT_TRUE((double)stats[i] / key_num < 1.0 / (slot_num - 1) &&
                    (double)stats[i] / key_num > 1.0 / (slot_num + 1));
    }
}

TEST_F(PartitionerTest, BalancedStrategyTest) {
    int slot_num = 4;
    BalancedPartitionStrategy partition_strategy(slot_num, 100, BalancedPartitionStrategy::kRange);
    int stats[slot_num];  // NOLINT
    for (int i = 0; i < slot_num; i++) {
        stats[i] = 0;
    }
    ASSERT_EQ(100, partition_strategy.hash_size_);
    ASSERT_EQ(slot_num, partition_strategy.assignments_->size());
    auto first_ass = dynamic_cast<SlotAssignment*>(partition_strategy.assignments_->at(0).get());
    ASSERT_TRUE(first_ass->IsAssigned(0));
    ASSERT_TRUE(first_ass->IsAssigned(24));
    ASSERT_FALSE(first_ass->IsAssigned(25));

    int key_num = 10000;
    for (int i = 0; i < key_num; i++) {
        std::string key = absl::StrCat("key_" + std::to_string(i));
        int hash0, hash1;
        int pid = partition_strategy.GetPartitionId(key, &hash0);
        auto pid_set = partition_strategy.GetPartitionSet(key, &hash1);
        auto pid_set2 = partition_strategy.GetPartitionSet(hash1);
        ASSERT_EQ(1, pid_set.size());
        ASSERT_EQ(pid, pid_set.at(0));
        ASSERT_EQ(hash0, hash1);
        ASSERT_EQ(pid_set, pid_set2);
        auto pid_set1 = partition_strategy.GetPartitionSet(hash0);
        ASSERT_EQ(pid_set1, pid_set);
        EXPECT_TRUE(pid >= 0 && pid < slot_num);
        stats[pid]++;
    }
    for (int i = 0; i < slot_num; i++) {
        EXPECT_TRUE((double)stats[i] / key_num < 1.0 / (slot_num - 1) &&
                    (double)stats[i] / key_num > 1.0 / (slot_num + 1));
    }

    std::shared_ptr<std::vector<std::shared_ptr<Assignment>>> assignments =
        std::make_shared<std::vector<std::shared_ptr<Assignment>>>(slot_num);
    for (int i = 0; i < slot_num; i++) {
        assignments->at(i) = std::make_shared<SlotAssignment>();
    }
    (*assignments)[0]->Assign(Range(0, 99));
    (*assignments)[1]->Assign(Range(0, 49));
    (*assignments)[2]->Assign(Range(50, 74));
    (*assignments)[3]->Assign(Range(70, 80));
    partition_strategy.UpdateAssignment(assignments);

    // partition id set:
    // [0, 49]: 0, 1
    // [50, 69]: 0, 2
    // [70, 74]: 0, 2, 3
    // [75, 80]: 0, 3
    // [81, 99]: 0
    for (int i = 0; i < key_num; i++) {
        std::string key = absl::StrCat("key_" + std::to_string(i));
        int hash = std::hash<std::string>()(key) % 100;
        int pid;
        int hash1;
        auto pid_set = partition_strategy.GetPartitionSet(key, &hash1);
        ASSERT_EQ(hash, hash1);
        if (hash >= 0 && hash <= 24) {
            std::vector<int> exp{0, 1};
            ASSERT_EQ(exp, pid_set);
        } else if (hash >= 25 && hash <= 49) {
            std::vector<int> exp{0, 1};
            ASSERT_EQ(exp, pid_set);
        } else if (hash >= 50 && hash <= 69) {
            std::vector<int> exp{0, 2};
            ASSERT_EQ(exp, pid_set);
        } else if (hash >= 70 && hash <= 74) {
            std::vector<int> exp{0, 2, 3};
            ASSERT_EQ(exp, pid_set);
        } else if (hash >= 75 && hash <= 80) {
            std::vector<int> exp{0, 3};
            ASSERT_EQ(exp, pid_set);
        } else if (hash >= 81 && hash <= 99) {
            std::vector<int> exp{0};
            ASSERT_EQ(exp, pid_set);
        } else {
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(PartitionerTest, BalancedStrategyScheduleTest) {
    const int slot_num = 4, hash_num = 16;
    FLAGS_stat_damping_factor = 0;
    BalancedPartitionStrategy strategy(slot_num, hash_num, BalancedPartitionStrategy::kRange);
    unsigned seed = 0;
    int r = rand_r(&seed);

    const int mac_rand = 520932930;
    bool in_mac = false;

    if (r == mac_rand) {
        in_mac = true;
        LOG(INFO) << "In MacOs";
    } else {
        LOG(INFO) << "In other os";
    }
    int key_num = 100;
    int target_score = key_num / slot_num;

    {
        SimpleDataStream stream(key_num, key_num);
        Partitioner partitioner(&stream, &strategy);
        std::shared_ptr<RingBuffer> buffers[slot_num];
        for (int i = 0; i < slot_num; i++) {
            buffers[i] = std::make_shared<RingBuffer>(key_num);
            partitioner.AddBuffer(buffers[i]);
        }
        partitioner.Run();
        partitioner.Join();
        EXPECT_EQ(key_num, partitioner.GetProcessedCounter());
        int count0 = 0, count1 = 0;
        std::vector<int> stats(slot_num, 0);
        auto& eles = stream.elements();
        ASSERT_EQ(key_num, eles.size());
        for (int i = 0; i < eles.size(); i++) {
            stats[eles[i]->pid()]++;
        }
        std::vector<int> exp;
        // in centos, rand_r has different behavior
        if (in_mac) {
            exp = {23, 30, 29, 18};
        } else {
            exp = {32, 27, 20, 21};
        }
        for (int i = 0; i < slot_num; i++) {
            ASSERT_EQ(buffers[i]->count(), stats[i]);
            LOG(INFO) << "pid: " << i << ", assigned: " << stats[i];
            ASSERT_EQ(exp[i], stats[i]);
        }
        auto dev = BalancedPartitionStrategy::Stddev(stats, target_score);
        LOG(INFO) << "dev = " << dev;
        ASSERT_GT(dev, 0.1);
    }

    ASSERT_EQ(0, strategy.UpdateSchedule());
    {
        SimpleDataStream stream(key_num, key_num);
        Partitioner partitioner(&stream, &strategy);
        std::shared_ptr<RingBuffer> buffers[slot_num];
        for (int i = 0; i < slot_num; i++) {
            buffers[i] = std::make_shared<RingBuffer>(key_num);
            partitioner.AddBuffer(buffers[i]);
        }
        partitioner.Run();
        partitioner.Join();
        EXPECT_EQ(key_num, partitioner.GetProcessedCounter());
        int count0 = 0, count1 = 0;
        std::vector<int> stats(slot_num, 0);
        auto& eles = stream.elements();
        ASSERT_EQ(key_num, eles.size());
        for (int i = 0; i < eles.size(); i++) {
            stats[eles[i]->pid()]++;
        }
        std::vector<int> exp;
        // in centos, rand_r has different behavior
        if (in_mac) {
            exp = {23, 27, 25, 25};
        } else {
            exp = {26, 25, 26, 23};
        }
        for (int i = 0; i < slot_num; i++) {
            ASSERT_EQ(buffers[i]->count(), stats[i]);
            LOG(INFO) << "pid: " << i << ", assigned: " << stats[i];
            ASSERT_EQ(exp[i], stats[i]) << "pid: " << i << ", assigned: " << stats[i];
        }
        auto dev = BalancedPartitionStrategy::Stddev(stats, target_score);
        ASSERT_LT(dev, 0.1) << "dev = " << dev;
    }

    // workload no change, updateschedule does not re-arrange the assignment
    ASSERT_EQ(1, strategy.UpdateSchedule());
    {
        int count = 1000;
        target_score = count / slot_num;
        SimpleDataStream stream(count, key_num);
        Partitioner partitioner(&stream, &strategy);
        std::shared_ptr<RingBuffer> buffers[slot_num];
        for (int i = 0; i < slot_num; i++) {
            buffers[i] = std::make_shared<RingBuffer>(count);
            partitioner.AddBuffer(buffers[i]);
        }
        partitioner.Run();
        partitioner.Join();
        EXPECT_EQ(count, partitioner.GetProcessedCounter());
        int count0 = 0, count1 = 0;
        std::vector<int> stats(slot_num, 0);
        auto& eles = stream.elements();
        ASSERT_EQ(count, eles.size());
        for (int i = 0; i < eles.size(); i++) {
            stats[eles[i]->pid()]++;
        }
        // the exp is different because there is randomness for choosing which pid
        // if there are multiple pids covering same hash
        std::vector<int> exp;
        if (in_mac) {
            exp = {233, 255, 256, 256};
        } else {
            exp = {246, 261, 258, 235};
        }
        for (int i = 0; i < slot_num; i++) {
            ASSERT_EQ(buffers[i]->count(), stats[i]);
            LOG(INFO) << "pid: " << i << ", assigned: " << stats[i];
            ASSERT_EQ(exp[i], stats[i]) << "pid: " << i << ", assigned: " << stats[i];
        }
        auto dev = BalancedPartitionStrategy::Stddev(stats, target_score);
        ASSERT_LT(dev, 0.1) << "dev = " << dev;
    }
}

TEST_F(PartitionerTest, RangeAssignmentTest) {
    RangeAssignment ass;
    ASSERT_FALSE(ass.IsAssigned(0));

    Range range(0, 100);
    ass.Assign(range);
    ASSERT_EQ(1, ass.ranges_.size());
    ASSERT_TRUE(ass.IsAssigned(0));
    ASSERT_TRUE(ass.IsAssigned(100));
    ASSERT_TRUE(ass.IsAssigned(50));
    ASSERT_FALSE(ass.IsAssigned(-1));
    ASSERT_FALSE(ass.IsAssigned(101));

    // duplicate range will be merged
    ass.Assign(range);
    ASSERT_EQ(1, ass.ranges_.size());

    {
        // subrange will be merged too
        Range range(50, 100);
        ass.Assign(range);
        ASSERT_EQ(1, ass.ranges_.size());
        ASSERT_EQ(0, ass.ranges_.at(100).start);
        ASSERT_EQ(100, ass.ranges_.at(100).end);
    }

    {
        // overlapped range will be merged too
        Range range(50, 150);
        ass.Assign(range);
        ASSERT_EQ(1, ass.ranges_.size());
        ASSERT_EQ(0, ass.ranges_.at(150).start);
        ASSERT_EQ(150, ass.ranges_.at(150).end);
    }

    {
        // overlapped range will be merged too
        Range range(-50, 150);
        ass.Assign(range);
        ASSERT_EQ(1, ass.ranges_.size());
        ASSERT_EQ(-50, ass.ranges_.at(150).start);
        ASSERT_EQ(150, ass.ranges_.at(150).end);
    }

    {
        // overlapped range will be merged too
        Range range(-100, 200);
        ass.Assign(range);
        ASSERT_EQ(1, ass.ranges_.size());
        ASSERT_EQ(-100, ass.ranges_.at(200).start);
        ASSERT_EQ(200, ass.ranges_.at(200).end);
    }

    // till here, the range will be [-100, 200]
    Range range2(300, 400);
    ass.Assign(range2);
    Range range3(600, 700);
    ass.Assign(range3);
    // till here, the range will be [-100, 200], [300, 400], [600, 700]

    ASSERT_EQ(3, ass.ranges_.size());
    ASSERT_EQ(-100, ass.ranges_.at(200).start);
    ASSERT_EQ(200, ass.ranges_.at(200).end);
    ASSERT_EQ(300, ass.ranges_.at(400).start);
    ASSERT_EQ(400, ass.ranges_.at(400).end);

    ASSERT_TRUE(ass.IsAssigned(200));
    ASSERT_TRUE(ass.IsAssigned(-100));
    ASSERT_TRUE(ass.IsAssigned(50));
    ASSERT_FALSE(ass.IsAssigned(-101));
    ASSERT_FALSE(ass.IsAssigned(201));

    ASSERT_TRUE(ass.IsAssigned(300));
    ASSERT_TRUE(ass.IsAssigned(400));
    ASSERT_TRUE(ass.IsAssigned(350));
    ASSERT_FALSE(ass.IsAssigned(401));
    ASSERT_FALSE(ass.IsAssigned(299));

    ASSERT_TRUE(ass.IsAssigned(600));
    ASSERT_TRUE(ass.IsAssigned(700));
    ASSERT_TRUE(ass.IsAssigned(601));
    ASSERT_FALSE(ass.IsAssigned(500));
    ASSERT_FALSE(ass.IsAssigned(800));

    // till here, the range will be [-100, 200], [300, 400], [600, 700]
    Range range4(400, 600);
    ass.Assign(range4);
    // till here, the range will be [-100, 200], [300, 700]
    ASSERT_EQ(2, ass.ranges_.size());
    ASSERT_EQ(-100, ass.ranges_.at(200).start);
    ASSERT_EQ(200, ass.ranges_.at(200).end);
    ASSERT_EQ(300, ass.ranges_.at(700).start);
    ASSERT_EQ(700, ass.ranges_.at(700).end);

    // till here, the range will be [-100, 200], [300, 700]
    Range range5(-200, 800);
    ass.Assign(range5);
    // till here, the range will be [-200, 800]
    ASSERT_EQ(1, ass.ranges_.size());
    ASSERT_EQ(-200, ass.ranges_.at(800).start);
    ASSERT_EQ(800, ass.ranges_.at(800).end);

    ass.Assign(900);
    // till here, the range will be [-200, 800], [900, 900]
    ASSERT_EQ(2, ass.ranges_.size());
    ASSERT_EQ(-200, ass.ranges_.at(800).start);
    ASSERT_EQ(800, ass.ranges_.at(800).end);
    ASSERT_EQ(900, ass.ranges_.at(900).start);
    ASSERT_EQ(900, ass.ranges_.at(900).end);
    ASSERT_TRUE(ass.IsAssigned(900));
    ASSERT_FALSE(ass.IsAssigned(901));
    ASSERT_FALSE(ass.IsAssigned(899));
}

TEST_F(PartitionerTest, SlotAssignmentTest) {
    SlotAssignment ass;
    ASSERT_EQ(0, ass.Assign(0));
    ASSERT_TRUE(ass.IsAssigned(0));
    ASSERT_FALSE(ass.IsAssigned(1));

    ASSERT_EQ(0, ass.Assign(1));
    ASSERT_TRUE(ass.IsAssigned(1));

    Range range(2, 5);
    ASSERT_EQ(0, ass.Assign(range));
    ASSERT_TRUE(ass.IsAssigned(2));
    ASSERT_TRUE(ass.IsAssigned(3));
    ASSERT_TRUE(ass.IsAssigned(5));
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
