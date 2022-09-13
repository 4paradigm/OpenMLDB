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

#include <gtest/gtest.h>

#include "common/accuracy.h"
#include "common/clock.h"
#include "common/ring_buffer.h"
#include "common/count_process_function.h"
#include "common/sum_process_function.h"
#include "common/distinct_count_process_function.h"
#include "stream/simple_data_stream.h"
#include "stream/memory_data_stream.h"
#include "executor/key_partition_interval_join.h"
#include "executor/dynamic_interval_join.h"
#include "executor/partition_strategy.h"
#include "executor/partitioner.h"

namespace streaming {
namespace interval_join {

class IntervalJoinTest : public ::testing::TestWithParam<JoinStrategy> {};

std::unique_ptr<IntervalJoiner> CreateJoiner(JoinStrategy strategy, JoinConfiguration* config,
                                             const std::shared_ptr<Buffer>& buffer, DataStream* output,
                                             Clock* clock = &Clock::GetClock(), int id = 0,
                                             PartitionStrategy* partition_strategy = nullptr) {
    static HashPartitionStrategy partition_strategy1(1);
    if (partition_strategy == nullptr) {
        partition_strategy = &partition_strategy1;
    }
    switch (strategy) {
        case JoinStrategy::kKeyPartition:
            return std::make_unique<KeyPartitionIntervalJoiner>(config, buffer, output, id, clock);
        case JoinStrategy::kDynamic:
            return std::make_unique<DynamicIntervalJoiner>(config, buffer, output, partition_strategy, id, clock);
        default:
            LOG(WARNING) << "Unsupport join strategy";
            break;
    }
}

TEST_P(IntervalJoinTest, SmokeTest) {
    Clock clock(0);
    int ele_num = 100;
    int base_count = 0;

    JoinConfiguration join_configuration(100, 100, 100, OpType::Count);
    Element eles[ele_num];  // NOLINT
    std::shared_ptr<RingBuffer> buffer = std::make_shared<RingBuffer>(ele_num);
    for (int i = 0; i < ele_num; i++) {
        eles[i] = Element("key", "value", i);
        if (i % 3 == 0) {
            eles[i].set_type(ElementType::kBase);
            base_count++;
        } else {
            eles[i].set_type(ElementType::kProbe);
        }
        buffer->Put(&eles[i]);
    }
    MemoryDataStream output;
    JoinStrategy strategy = GetParam();
    auto joiner = CreateJoiner(strategy, &join_configuration, buffer, &output, &clock);
    joiner->Run();
    joiner->Join();
    ASSERT_EQ(output.size(), base_count);
    for (int i = 0; i < base_count; i++) {
        ASSERT_EQ(output.Get()->value(), std::to_string(i * 2));
    }
}

TEST_P(IntervalJoinTest, OverlappingTest) {
    Clock clock(0);
    int ele_num = 100;
    int window = 10;

    JoinConfiguration join_configuration(window, ele_num, -1, OpType::Sum, false, false, false, false, true);
    {
        int base_count = 0;
        Element eles[ele_num];  // NOLINT
        std::shared_ptr<RingBuffer> buffer = std::make_shared<RingBuffer>(ele_num);
        for (int i = 0; i < ele_num; i++) {
            eles[i] = Element("key", "1", i);
            if (i % 2 == 0) {
                eles[i].set_type(ElementType::kBase);
                base_count++;
            } else {
                eles[i].set_type(ElementType::kProbe);
            }
            buffer->Put(&eles[i]);
        }
        MemoryDataStream output;
        JoinStrategy strategy = GetParam();
        auto joiner = CreateJoiner(strategy, &join_configuration, buffer, &output, &clock);
        joiner->Run();
        joiner->Join();
        ASSERT_EQ(output.size(), base_count);
        for (int i = 0; i < base_count; i++) {
            double exp = i;
            exp = std::min(exp, window / 2.0);
            auto out = output.Get();
            ASSERT_EQ(out->value(), std::to_string(exp));
            ASSERT_EQ(out->ts(), i * 2);
        }
    }

    {
        int base_count = 0;
        Element eles[ele_num];  // NOLINT
        std::shared_ptr<RingBuffer> buffer = std::make_shared<RingBuffer>(ele_num);
        for (int i = 0; i < ele_num; i++) {
            eles[i] = Element("key", "1", i);
            if (i % 2 == 0) {
                eles[i].set_type(ElementType::kProbe);
                base_count++;
            } else {
                eles[i].set_type(ElementType::kBase);
            }
            buffer->Put(&eles[i]);
        }
        MemoryDataStream output;
        JoinStrategy strategy = GetParam();
        auto joiner = CreateJoiner(strategy, &join_configuration, buffer, &output, &clock);
        joiner->Run();
        joiner->Join();
        ASSERT_EQ(output.size(), base_count);
        for (int i = 1; i <= base_count; i++) {
            double exp = i;
            exp = std::min(exp, window / 2.0);
            auto out = output.Get();
            ASSERT_EQ(out->value(), std::to_string(exp));
            ASSERT_EQ(out->ts(), i * 2 - 1);
        }
    }
}

TEST_P(IntervalJoinTest, CleanupProbeTest) {
    Clock clock(0);
    int count = 0;
    std::shared_ptr<RingBuffer> buffer = std::make_shared<RingBuffer>(0);
    MemoryDataStream output;
    int window = 20;
    int lateness = 10;
    // every ProcessElement() will do the cleanup
    int cleanup_interval = 0;
    JoinConfiguration join_configuration(window, lateness, cleanup_interval, OpType::Count);
    JoinStrategy strategy = GetParam();
    auto joiner = CreateJoiner(strategy, &join_configuration, buffer, &output, &clock);
    int ele_num = 100;
    Element eles[ele_num];  // NOLINT
    for (int i = 0; i < ele_num; i++) {
        eles[i] = Element("key", "value", i);
        if (i % 2 == 0) {
            eles[i].set_type(ElementType::kBase);
            joiner->ProcessElement(&eles[i]);
            count++;
        } else {
            eles[i].set_type(ElementType::kProbe);
            joiner->ProcessElement(&eles[i]);
        }
        clock.UpdateTime(i);
    }

    for (int i = 0; i < count; i++) {
        int max_val = window / 2;
        int exp = i > max_val ? max_val : i;
        ASSERT_EQ(std::to_string(exp), output.Get()->value()) << i << " failed ";
    }

    // out of order case
    // now the remaining probe elements: [69, 99]
    Element ele_1("key", "value", 67, ElementType::kBase);
    joiner->ProcessBaseElement(&ele_1);
    ASSERT_EQ(output.Get()->value(), "0");

    Element ele_2("key", "value", 81, ElementType::kBase);
    joiner->ProcessBaseElement(&ele_2);
    // 69, 71, 73, ..., 81
    ASSERT_EQ(output.Get()->value(), "7");

    Element ele_3("key", "value", 99, ElementType::kBase);
    joiner->ProcessBaseElement(&ele_3);
    // 79, 81, 83, ..., 99
    ASSERT_EQ(output.Get()->value(), "11");
    ASSERT_EQ(nullptr, output.Get());
}

TEST_P(IntervalJoinTest, CleanupBaseTest) {
    Clock clock(0);
    int count = 0;
    std::shared_ptr<RingBuffer> buffer = std::make_shared<RingBuffer>(0);
    MemoryDataStream output;
    int window = 20;
    int lateness = 10;
    // every ProcessElement() will do the cleanup
    int cleanup_interval = 0;
    JoinConfiguration join_configuration(window, lateness, cleanup_interval, OpType::Count);
    JoinStrategy strategy = GetParam();
    auto joiner = CreateJoiner(strategy, &join_configuration, buffer, &output, &clock);
    int ele_num = 100;
    Element eles[ele_num];  // NOLINT
    for (int i = 0; i < ele_num; i++) {
        clock.UpdateTime(i);

        eles[i] = Element("key", "value", i);
        if (i % 2 == 0) {
            eles[i].set_type(ElementType::kBase);
            joiner->ProcessElement(&eles[i]);
            count++;
        } else {
            eles[i].set_type(ElementType::kProbe);
            joiner->ProcessElement(&eles[i]);
        }
    }

    for (int i = 0; i < count; i++) {
        int max_val = window / 2;
        int exp = i > max_val ? max_val : i;
        ASSERT_EQ(std::to_string(exp), output.Get()->value()) << i << " failed ";
    }

    // out of order case
    // now the remaining base elements: even numbers in [89, 99]
    Element ele_1("key", "value", 101, ElementType::kProbe);
    joiner->ProcessProbeElement(&ele_1);
    ASSERT_EQ(nullptr, output.Get());

    Element ele_2("key", "value", 78, ElementType::kProbe);
    joiner->ProcessProbeElement(&ele_2);
    // the base elements of 90, 92, ..., 98 will be updated
    ASSERT_EQ(5, output.size());
    count = 0;
    std::set<int> exp_ts = {90, 92, 94, 96, 98};
    while (auto ele = output.Get()) {
        ASSERT_EQ("key", ele->key());
        ASSERT_TRUE(exp_ts.count(ele->ts()));
        ASSERT_EQ("11", ele->value());
        count++;
    }
    ASSERT_EQ(5, count);

    Element ele_3("key", "value", 98, ElementType::kProbe);
    joiner->ProcessProbeElement(&ele_3);
    auto ele =  output.Get();
    ASSERT_EQ("key", ele->key());
    ASSERT_EQ(98, ele->ts());
    ASSERT_EQ("12", ele->value());
    ASSERT_EQ(nullptr, output.Get());

    // the boundary == the cleanup bound
    // now the remaining base elements: even numbers in [90, 100]
    clock.UpdateTime(100);
    joiner->Cleanup();
    Element ele_22("key", "value", 78, ElementType::kProbe);
    joiner->ProcessProbeElement(&ele_22);
    // the base elements of 90, 92, ..., 98 will be updated
    ASSERT_EQ(5, output.size());
    count = 0;
    while (auto ele = output.Get()) {
        ASSERT_EQ("key", ele->key());
        ASSERT_TRUE(exp_ts.count(ele->ts()));
        // 98 update one extra time as we call joiner->ProcessProbeElement(&ele_3) additionally
        if (ele->ts() == 98) {
            ASSERT_EQ("13", ele->value());
        } else {
            ASSERT_EQ("12", ele->value());
        }
        count++;
    }
    ASSERT_EQ(5, count);

    Element ele_33("key", "value", 98, ElementType::kProbe);
    joiner->ProcessProbeElement(&ele_33);
    ele =  output.Get();
    ASSERT_EQ("key", ele->key());
    ASSERT_EQ(98, ele->ts());
    ASSERT_EQ("14", ele->value());
    ASSERT_EQ(nullptr, output.Get());
}

#if defined(SLOT_DUPLICATE_CHECKING) || defined(LOCK_DUCPLICATE_CHECKING)
TEST_F(IntervalJoinTest, DuplicateProbeTest) {
    Clock clock(0);
    int count = 0;
    std::shared_ptr<RingBuffer> buffer = std::make_shared<RingBuffer>(0);
    MemoryDataStream output;
    int window = 20;
    int lateness = 10;
    // every ProcessElement() will do the cleanup
    int cleanup_interval = 0;
    JoinConfiguration join_configuration(window, lateness, cleanup_interval, OpType::Count);
    JoinStrategy strategy = JoinStrategy::kDynamic;
    auto joiner = CreateJoiner(strategy, &join_configuration, buffer, &output, &clock);
    int ele_num = 100;
    Element eles[ele_num];  // NOLINT
    for (int i = 0; i < ele_num; i++) {
        eles[i] = Element("key", "value", i);
        if (i % 2 == 0) {
            eles[i].set_type(ElementType::kBase);
            joiner->ProcessElement(&eles[i]);
            count++;
        } else {
            eles[i].set_type(ElementType::kProbe);
            joiner->ProcessElement(&eles[i]);
            // process same element twice will be ignored for DynamicJoiner
            joiner->ProcessElement(&eles[i]);
        }
        clock.UpdateTime(i);
    }

    for (int i = 0; i < count; i++) {
        int max_val = window / 2;
        int exp = i > max_val ? max_val : i;
        ASSERT_EQ(std::to_string(exp), output.Get()->value()) << i << " failed ";
    }
}
#endif

TEST_F(IntervalJoinTest, SnapshotTest) {
    Clock clock(0);
    int count = 0;
    std::shared_ptr<RingBuffer> buffer = std::make_shared<RingBuffer>(0);
    MemoryDataStream output;
    int ele_num = 100;
    int window = 1000;
    int lateness = 10;
    // no cleanup
    int cleanup_interval = -1;
    JoinConfiguration join_configuration(window, lateness, cleanup_interval, OpType::Count);
    JoinStrategy strategy = JoinStrategy::kDynamic;
    std::unique_ptr<DynamicIntervalJoiner> joiner(dynamic_cast<DynamicIntervalJoiner*>(
        CreateJoiner(strategy, &join_configuration, buffer, &output, &clock).release()));
    Element eles[ele_num];  // NOLINT
    Element non_visible_eles[ele_num];  // NOLINT
    for (int i = 0; i < ele_num; i++) {
        eles[i] = Element("key", "value", i);

        // not visible to the process
        non_visible_eles[i] = Element("key", "value", i);
        non_visible_eles[i].set_id(i + 1 + ele_num);
        LOG(WARNING) << "non visible ele " << non_visible_eles[i].ToString();
        if (i % 2 == 0) {
            non_visible_eles[i].set_type(ElementType::kProbe);
            joiner->processed_probe_.Put("key", i, &non_visible_eles[i], false);
            eles[i].set_type(ElementType::kBase);
            joiner->ProcessElement(&eles[i]);
            count++;
        } else {
            non_visible_eles[i].set_type(ElementType::kBase);
            joiner->processed_base_.Put("key", i, &non_visible_eles[i], false);
            eles[i].set_type(ElementType::kProbe);
            joiner->ProcessElement(&eles[i]);
        }
        clock.UpdateTime(i);
    }


    Element base("key", "value", ele_num - 1, ElementType::kBase);
    SkipListTable::id_ = ele_num * 2;
    joiner->ProcessElement(&base);

    for (int i = 0; i < count; i++) {
        int exp = i;
        ASSERT_EQ(std::to_string(exp), output.Get()->value()) << i << " failed ";
    }

    int exp = ele_num;
    auto out = output.Get();
    ASSERT_EQ(std::to_string(exp), out->value()) << out->ToString() << " failed ";
}

TEST_P(IntervalJoinTest, LateCheckTest) {
    Clock clock(100);
    // late arrival
    int ele_num = 10;
    int window = 5;
    std::shared_ptr<RingBuffer> buffer = std::make_shared<RingBuffer>(ele_num);
    MemoryDataStream output;
    JoinConfiguration join_configuration(window, 50, 100, OpType::Count, false);
    JoinStrategy strategy = GetParam();
    auto joiner = CreateJoiner(strategy, &join_configuration, buffer, &output, &clock);

    // late arrival will be skipped under normal cases
    Element bele("key", "value", 0);
    bele.set_type(ElementType::kBase);
    ASSERT_EQ(joiner->ProcessElement(&bele), 1);
    Element pele("key", "value", 0);
    pele.set_type(ElementType::kProbe);
    ASSERT_EQ(joiner->ProcessElement(&pele), 1);

    // late arrival will be processed if late_check is false
    join_configuration.late_check = false;
    // out of order
    Element eles[ele_num];  // NOLINT
    for (int i = 0; i < ele_num; i++) {
        eles[i] = Element("key", "value", i);
        eles[i].set_type(ElementType::kBase);
        joiner->ProcessElement(&eles[i]);
    }
    ASSERT_EQ(output.size(), ele_num);

    joiner->ProcessElement(&pele);
    ASSERT_EQ(output.size(), ele_num + window + 1);

    for (int i = 0; i < ele_num; i++) {
        ASSERT_EQ(output.Get()->value(), "0");
    }

    for (int i = 0; i < window + 1; i++) {
        ASSERT_EQ(output.Get()->value(), "1");
    }
}

TEST_F(IntervalJoinTest, ProcessFunctionGetterTest) {
    Element ele("key", "value", 0);

    auto count_function = IntervalJoiner::GetProcessFunction(OpType::Count, &ele);
    ASSERT_TRUE(count_function != nullptr);
    ASSERT_TRUE(dynamic_cast<CountProcessFunction*>(count_function.get()) != nullptr);

    auto sum_function = IntervalJoiner::GetProcessFunction(OpType::Sum, &ele);
    ASSERT_TRUE(sum_function != nullptr);
    ASSERT_TRUE(dynamic_cast<SumProcessFunction*>(sum_function.get()) != nullptr);

    auto distinct_count_function = IntervalJoiner::GetProcessFunction(OpType::DistinctCount, &ele);
    ASSERT_TRUE(distinct_count_function != nullptr);
    ASSERT_TRUE(dynamic_cast<DistinctCountProcessFunction*>(distinct_count_function.get()) != nullptr);

    auto unknown_function = IntervalJoiner::GetProcessFunction(OpType::Unknown, &ele);
    ASSERT_TRUE(unknown_function == nullptr);
}

TEST_P(IntervalJoinTest, TimeWindowTest) {
    int window = 10;
    std::shared_ptr<RingBuffer> buffer = std::make_shared<RingBuffer>(0);
    MemoryDataStream output;
    JoinConfiguration join_configuration(window, 100, 100, OpType::Unknown);
    JoinStrategy strategy = GetParam();
    auto joiner = CreateJoiner(strategy, &join_configuration, buffer, &output);
    ASSERT_TRUE(joiner->IsInTimeWindow(1, 0));
    ASSERT_FALSE(joiner->IsInTimeWindow(0, 1));
    ASSERT_FALSE(joiner->IsInTimeWindow(0, window + 1));
    ASSERT_FALSE(joiner->IsInTimeWindow(window + 1, 0));
}

TEST_P(IntervalJoinTest, LatenessTest) {
    Clock clock(100);
    std::shared_ptr<RingBuffer> buffer = std::make_shared<RingBuffer>(0);
    MemoryDataStream output;
    JoinConfiguration join_configuration(100, 50, 100, OpType::Unknown);
    JoinStrategy strategy = GetParam();
    auto joiner = CreateJoiner(strategy, &join_configuration, buffer, &output, &clock);
    ASSERT_TRUE(joiner->IsLate(0));
    ASSERT_TRUE(joiner->IsLate(40));
    ASSERT_FALSE(joiner->IsLate(60));
    ASSERT_FALSE(joiner->IsLate(110));
}

TEST_P(IntervalJoinTest, MultipleJoinerTest) {
    Clock clock(0);
    int ts_num = 100;
    int key_num = 100;
    int base_count = 0;

    JoinConfiguration join_configuration(100, 100, 100, OpType::Count);
    std::vector<std::vector<Element>> eles(key_num);
    for (int i = 0; i < key_num; i++) {
        eles[i].resize(ts_num);
    }
    std::shared_ptr<RingBuffer> buffer0 = std::make_shared<RingBuffer>(ts_num * key_num);
    std::shared_ptr<RingBuffer> buffer1 = std::make_shared<RingBuffer>(ts_num * key_num);
    MemoryDataStream input;

    for (int i = 0; i < ts_num; i++) {
        for (int k = 0; k < key_num; k++) {
            auto key = absl::StrCat("key_", k);
            eles[k][i] = Element(key, "value", i);
            if (i % 2 == 0) {
                eles[k][i].set_type(ElementType::kBase);
                base_count++;
            } else {
                eles[k][i].set_type(ElementType::kProbe);
            }
            input.Put(&eles[k][i]);
        }
    }
    HashPartitionStrategy partition_strategy(2);
    Partitioner partitioner0(&input, &partition_strategy);
    partitioner0.AddBuffer(buffer0);
    partitioner0.AddBuffer(buffer1);
    partitioner0.Run();

    MemoryDataStream output0;
    MemoryDataStream output1;
    JoinStrategy strategy = GetParam();
    auto joiner0 = CreateJoiner(strategy, &join_configuration, buffer0, &output0, &clock, 0, &partition_strategy);
    auto joiner1 = CreateJoiner(strategy, &join_configuration, buffer1, &output1, &clock, 1, &partition_strategy);
    joiner0->Run();
    joiner1->Run();

    partitioner0.Join();
    joiner0->Join();
    joiner1->Join();
    MergedMemoryDataStream output({&output0, &output1});
    LOG(WARNING) << "base_count = " << base_count;
    ASSERT_EQ(output.size(), base_count);
    for (int k = 0; k < key_num; k++) {
        for (int i = 0; i < base_count / key_num; i++) {
            auto val = output.Get();
            int exp;
            if (val->ts() > 100) {
                exp = 50;
            } else {
                exp = val->ts() / 2;
            }
            ASSERT_EQ(std::to_string(exp), val->value()) << "key: " << val->key() << ", i: " << i;
        }
    }
}

TEST_F(IntervalJoinTest, DynamicJoinerTest) {
    Clock clock(0);
    int ts_num = 10000;
    int key_num = 100;
    int base_count = 0;

    JoinConfiguration join_configuration(100, 100, 100, OpType::Count);
    std::vector<std::vector<Element>> eles(key_num);
    for (int i = 0; i < key_num; i++) {
        eles[i].resize(ts_num);
    }
    std::shared_ptr<RingBuffer> buffer0 = std::make_shared<RingBuffer>(ts_num * key_num);
    std::shared_ptr<RingBuffer> buffer1 = std::make_shared<RingBuffer>(ts_num * key_num);
    MemoryDataStream input;

    for (int i = 0; i < ts_num; i++) {
        for (int k = 0; k < key_num; k++) {
            auto key = absl::StrCat("key_", k);
            eles[k][i] = Element(key, "value", i);
            if (i % 2 == 0) {
                eles[k][i].set_type(ElementType::kBase);
                base_count++;
            } else {
                eles[k][i].set_type(ElementType::kProbe);
            }
            input.Put(&eles[k][i]);
        }
    }
    BalancedPartitionStrategy partition_strategy(2, 10);
    std::shared_ptr<std::vector<std::shared_ptr<Assignment>>> assignments =
        std::make_shared<std::vector<std::shared_ptr<Assignment>>>(2);
    assignments->at(0) = std::make_shared<RangeAssignment>();
    assignments->at(1) = std::make_shared<RangeAssignment>();
    assignments->at(0)->Assign(Range(0, 8));
    assignments->at(1)->Assign(Range(5, 9));
    partition_strategy.UpdateAssignment(assignments);

    Partitioner partitioner0(&input, &partition_strategy);
    partitioner0.AddBuffer(buffer0);
    partitioner0.AddBuffer(buffer1);
    partitioner0.Run();

    MemoryDataStream output0;
    MemoryDataStream output1;
    JoinStrategy strategy = JoinStrategy::kDynamic;
    auto joiner0 = CreateJoiner(strategy, &join_configuration, buffer0, &output0, &clock, 0, &partition_strategy);
    auto joiner1 = CreateJoiner(strategy, &join_configuration, buffer1, &output1, &clock, 1, &partition_strategy);
    DynamicIntervalJoiner* joiner0_p = dynamic_cast<DynamicIntervalJoiner*>(joiner0.get());
    DynamicIntervalJoiner* joiner1_p = dynamic_cast<DynamicIntervalJoiner*>(joiner1.get());
    joiner0_p->SetPeerJoiners({joiner0_p, joiner1_p});
    joiner1_p->SetPeerJoiners({joiner0_p, joiner1_p});
    joiner0->Run();
    joiner1->Run();

    partitioner0.Join();
    joiner0->Join();
    joiner1->Join();
    MergedMemoryDataStream output({&output0, &output1});
    LOG(WARNING) << "base_count = " << base_count;
    ASSERT_GE(output.size(), base_count);
    std::vector<std::shared_ptr<Element>> correct_res;
    for (int i = 0; i < ts_num; i++) {
        for (int k = 0; k < key_num; k++) {
            if (i % 2 == 0) {
                auto& ele = eles[k][i];
                int exp;
                if (ele.ts() > 100) {
                    exp = 50;
                } else {
                    exp = ele.ts() / 2;
                }
                correct_res.push_back(std::make_shared<Element>(ele.key(), std::to_string(exp), ele.ts()));
            }
        }
    }
    double correctness = CalculateAccuracy(correct_res, output.elements());
    LOG(WARNING) << "correctness: " << correctness;
    ASSERT_EQ(1.0, correctness);
}

INSTANTIATE_TEST_CASE_P(TestJoinStrategies, IntervalJoinTest,
                        ::testing::Values(JoinStrategy::kKeyPartition, JoinStrategy::kDynamic));

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
