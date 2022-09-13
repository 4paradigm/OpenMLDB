/*
 * Copyright 2021 4Paradigm
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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <common/timer.h>

#include <iostream>
#include <fstream>
#include <memory>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include "common/ring_buffer.h"
#include "executor/interval_join.h"
#include "executor/key_partition_interval_join.h"
#include "executor/partitioner.h"
#include "executor/single_thread_executor.h"
#include "stream/memory_data_stream.h"
#include "stream/simple_data_stream.h"
#include "common/switch_buffer.h"
#include "executor/dynamic_interval_join.h"
#include "executor/scheduler.h"
#include "common/accuracy.h"
#include "common/util.h"

DEFINE_int64(base_stream_count, 5000, "number of elements in the base stream");
DEFINE_int64(probe_stream_count, 10000, "number of elements in the probe stream");
DEFINE_int32(key_num, 10, "number of keys in the stream");
DEFINE_string(stream_type, "simple", "stream source type");
DEFINE_string(base_file, "data/input.csv", "base stream_file path if stream type == file");
DEFINE_string(base_key, "key", "column name for key in base stream if stream type == file");
DEFINE_string(base_val, "val", "column name for value in base stream if stream type == file");
DEFINE_string(base_ts, "ts", "column name for timestamp in base stream if stream type == file");
DEFINE_string(probe_file, "data/input.csv", "probe stream_file path if stream type == file");
DEFINE_string(probe_key, "key", "column name for key in base stream if stream type == file");
DEFINE_string(probe_val, "val", "column name for value in base stream if stream type == file");
DEFINE_string(probe_ts, "ts", "column name for timestamp in base stream if stream type == file");
DEFINE_string(buffer_type, "ring", "stream source type [ring, switch]");
DEFINE_int32(buffer_size, 1024, "comm buffer size");
DEFINE_string(join_strategy, "kp", "interval join strategy");
DEFINE_string(partition_strategy, "hash", "partition strategy");
DEFINE_int32(partition_thread, 1, "number of threads used for partition");
DEFINE_int32(joiner_thread, 2, "number of threads used for partition");
DEFINE_int64(window, 1000, "size of time window for interval join (unit: microseconds)");
DEFINE_int64(lateness_us, -1, "time to delay before cleanup (unit: microseconds)");
DEFINE_int64(cleanup_interval, 1000, "time between two times of cleanup (unit: microseconds)");
DEFINE_string(op_type, "sum", "op type for interval join aggregation [count, sum, distinct_count]");
DEFINE_bool(cleanup, true, "whether do cleanup");
DEFINE_bool(late_check, true, "whether do late check");
DEFINE_int32(print_out_num, 10, "number of output to print out");
DEFINE_bool(time_record, true, "whether do time recording");
DEFINE_int32(hash_size, 1024, "the hash size of BalancedPartitionStrategy");
DEFINE_bool(latency_record, true, "whether do latency recording");
DEFINE_string(latency_output_file, "latency.csv", "file path for output of latency analysis");
DEFINE_bool(accuracy_check, true, "whether do accuracy check");
DEFINE_bool(overlapping, false, "whether use the overlapping technique");
DEFINE_bool(overlapping_record, false, "whether do overlapping recording");
DEFINE_bool(effectiveness_record, false, "whether do effectiveness recording");
DEFINE_string(cached_dir, "", "cached dir for result re-use");
DEFINE_double(arrive_rate, -1, "arrival rate of data stream");

using namespace streaming::interval_join;  // NOLINT

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    std::shared_ptr<Partitioner> partitioners[FLAGS_partition_thread];
    std::shared_ptr<IntervalJoiner> joiners[FLAGS_joiner_thread];
    std::shared_ptr<Buffer> buffers[FLAGS_joiner_thread];

    if (FLAGS_arrive_rate == -1) {
        FLAGS_arrive_rate_per_partitioner = -1;
    } else {
        FLAGS_arrive_rate_per_partitioner = FLAGS_arrive_rate / FLAGS_partition_thread;
    }

    std::shared_ptr<DataStream> ds;
    if (boost::iequals(FLAGS_stream_type, "simple")) {
        int64_t max_ts = std::max(FLAGS_base_stream_count, FLAGS_probe_stream_count);
        SimpleDataStream base(FLAGS_base_stream_count, FLAGS_key_num, max_ts, StreamType::kBase);
        SimpleDataStream probe(FLAGS_probe_stream_count, FLAGS_key_num, max_ts, StreamType::kProbe);
        ds.reset(new MergedMemoryDataStream({&probe, &base}));
    } else if (boost::iequals(FLAGS_stream_type, "file")) {
        MemoryDataStream base(StreamType::kBase);
        MemoryDataStream probe(StreamType::kProbe);
        base.ReadFromFile(FLAGS_base_file, FLAGS_base_key, FLAGS_base_val, FLAGS_base_ts);
        probe.ReadFromFile(FLAGS_probe_file, FLAGS_probe_key, FLAGS_probe_val, FLAGS_probe_ts);
        ds.reset(new MergedMemoryDataStream({&probe, &base}));
    } else {
        LOG(ERROR) << "Unsupported stream type: " << FLAGS_stream_type;
        return -1;
    }

    for (int i = 0; i < FLAGS_joiner_thread; i++) {
        if (boost::iequals(FLAGS_buffer_type, "ring")) {
            buffers[i].reset(new RingBuffer(FLAGS_buffer_size, FLAGS_partition_thread));
        } else if (boost::iequals(FLAGS_buffer_type, "switch")) {
            buffers[i].reset(new SwitchBuffer(FLAGS_buffer_size, FLAGS_partition_thread));
        } else {
            LOG(ERROR) << "Unsupported buffer type: " << FLAGS_buffer_type;
            return -1;
        }
    }

    std::unique_ptr<PartitionStrategy> partition_strategy;
    if (boost::iequals(FLAGS_partition_strategy, "hash")) {
        partition_strategy = std::make_unique<HashPartitionStrategy>(FLAGS_joiner_thread);
    } else if (boost::iequals(FLAGS_partition_strategy, "balanced")) {
        if (boost::iequals(FLAGS_join_strategy, "kp")) {
            LOG(ERROR) << "kp join strategy doesn't support 'balanced` partition strategy";
            return -1;
        }
        partition_strategy = std::make_unique<BalancedPartitionStrategy>(FLAGS_joiner_thread, FLAGS_hash_size);
    } else {
        LOG(ERROR) << "Unsupported join strategy: " << FLAGS_join_strategy;
        return -1;
    }

    for (int i = 0; i < FLAGS_partition_thread; i++) {
        partitioners[i].reset(new Partitioner(ds.get(), partition_strategy.get(), i, FLAGS_latency_record));
        for (int j = 0; j < FLAGS_joiner_thread; j++) {
            partitioners[i]->AddBuffer(buffers[j]);
        }
    }

    OpType op;
    if (boost::iequals(FLAGS_op_type, "count")) {
        op = OpType::Count;
    } else if (boost::iequals(FLAGS_op_type, "sum")) {
        op = OpType::Sum;
    } else if (boost::iequals(FLAGS_op_type, "distinct_count")) {
        op = OpType::DistinctCount;
    } else {
        op = OpType::Unknown;
    }

    if (FLAGS_lateness_us == -1) {
        FLAGS_lateness_us = FLAGS_buffer_size * (FLAGS_joiner_thread + 1);
    }

    JoinConfiguration join_configuration(FLAGS_window, FLAGS_lateness_us, FLAGS_cleanup_interval, op, FLAGS_cleanup,
                                         FLAGS_late_check, FLAGS_time_record, FLAGS_latency_record, FLAGS_overlapping,
                                         FLAGS_overlapping_record, FLAGS_effectiveness_record);
    std::vector<MemoryDataStream> outputs(FLAGS_joiner_thread);

    for (int i = 0; i < FLAGS_joiner_thread; i++) {
        if (boost::iequals(FLAGS_join_strategy, "kp")) {
            joiners[i].reset(new KeyPartitionIntervalJoiner(&join_configuration, buffers[i], &outputs[i], i));
        } else if (boost::iequals(FLAGS_join_strategy, "dynamic")) {
            joiners[i].reset(
                new DynamicIntervalJoiner(&join_configuration, buffers[i], &outputs[i], partition_strategy.get(), i));
        } else {
            LOG(ERROR) << "Unsupported join strategy: " << FLAGS_join_strategy;
            return -1;
        }
    }
    if (boost::iequals(FLAGS_join_strategy, "dynamic")) {
        std::vector<const DynamicIntervalJoiner*> peers(FLAGS_joiner_thread);
        for (int i = 0; i < FLAGS_joiner_thread; i++) {
            peers[i] = dynamic_cast<DynamicIntervalJoiner*>(joiners[i].get());
        }

        for (int i = 0; i < FLAGS_joiner_thread; i++) {
            dynamic_cast<DynamicIntervalJoiner*>(joiners[i].get())->SetPeerJoiners(peers);
        }
    }
    Scheduler scheduler(partition_strategy.get());
    // start the scheduler
    scheduler.Run();

    // start all executors
    // start all partitioners
    LOG(INFO) << "Starting " << FLAGS_partition_thread << " partitioners";
    for (int i = 0; i < FLAGS_partition_thread; i++) {
        partitioners[i]->Run();
    }
    usleep(50000);

    // start all joiners
    LOG(INFO) << "Starting " << FLAGS_joiner_thread << " join executors";
    for (int i = 0; i < FLAGS_joiner_thread; i++) {
        joiners[i]->Run();
    }

    auto start = ::baidu::common::timer::get_micros();
    // wait for all executors to complete
    size_t partitioner_processed = 0;
    for (int i = 0; i < FLAGS_partition_thread; i++) {
        partitioners[i]->Join();
        LOG(INFO) << "Partitioner " << i << " processed " << partitioners[i]->GetProcessedCounter() << " elements";
        partitioner_processed += partitioners[i]->GetProcessedCounter();
    }
    LOG(INFO) << "All partitioners processed " << partitioner_processed << " elements in total";

    // mark all buffers as done
    for (int i = 0; i < FLAGS_joiner_thread; i++) {
        buffers[i]->MarkDone();
    }

    size_t joiner_processed = 0;
    std::vector<double> scores(FLAGS_joiner_thread);
    for (int i = 0; i < FLAGS_joiner_thread; i++) {
        joiners[i]->Join();
        LOG(INFO) << "joiner " << i << " processed " << joiners[i]->GetProcessedCounter() << " elements";
        joiner_processed += joiners[i]->GetProcessedCounter();
        scores[i] = joiners[i]->GetProcessedCounter();
    }
    double balancedness =
        BalancedPartitionStrategy::Stddev(scores, static_cast<double>(joiner_processed) / FLAGS_joiner_thread);
    LOG(INFO) << "Balancedness: " << balancedness;

    if (boost::iequals(FLAGS_stream_type, "simple")) {
        CHECK_EQ(FLAGS_base_stream_count + FLAGS_probe_stream_count, joiner_processed);
    }
    LOG(INFO) << "All joiners processed " << joiner_processed << " elements in total";

    auto end = ::baidu::common::timer::get_micros();
    auto elapsed_ms = (end - start - FLAGS_buffer_blocking_sleep_us) / 1000;
    double throughput =  static_cast<double>(FLAGS_base_stream_count + FLAGS_probe_stream_count) / elapsed_ms * 1000;
    LOG(INFO) << "Total elapsed time for processing: " << elapsed_ms << " ms";
    LOG(INFO) << "Throughput: " << throughput << " records/s";
    scheduler.Stop();
    scheduler.Join();

    if (FLAGS_time_record) {
        std::vector<TimeStatistics> stats;
        for (int i = 0; i < FLAGS_joiner_thread; i++) {
            stats.emplace_back(joiners[i]->statistics());
        }
        auto merged_stat = TimeStatistics::MergeStatistics(stats);
        merged_stat->Print();
    }

    std::vector<DataStream*> sep_streams(FLAGS_joiner_thread);
    for (int i = 0; i < FLAGS_joiner_thread; i++) {
        sep_streams[i] = &outputs[i];
    }
    MergedMemoryDataStream merged_output(sep_streams);
    LOG(INFO) << "OutputStream: " << merged_output.size() << " elements";

    auto out_eles = merged_output.elements();
    int64_t size = merged_output.size();
    if (FLAGS_print_out_num == -1) {
        FLAGS_print_out_num = size;
    }
    for (int i = 0; i < FLAGS_print_out_num; i++) {
        int idx = size - 1 - i;
        if (idx < 0) {
            break;
        }
        auto ele = out_eles[idx];
        LOG(INFO) << "Out[" << i << "]: " << (*ele);
    }

    if (FLAGS_latency_record) {
        LOG(INFO) << "Latency is: " << std::to_string(GetLatency(ds.get(), FLAGS_latency_output_file));
    }

    if (FLAGS_overlapping_record) {
        LOG(INFO) << "Average join count is: " << GetJoinCounts(ds.get());
    }

    if (FLAGS_effectiveness_record) {
        LOG(INFO) << "Effectiveness of base elements is: " << GetBaseEffectiveness(ds.get());
        LOG(INFO) << "Effectiveness of probe elements is: " << GetProbeEffectiveness(ds.get());
    }

    if (FLAGS_accuracy_check) {
        MemoryDataStream correct_output;
        std::string filename = absl::StrCat(
            "single_thread_key-", std::to_string(FLAGS_key_num), "_base-", std::to_string(FLAGS_base_stream_count),
            "_probe-", FLAGS_probe_stream_count, "_window-", FLAGS_window, "_op-", FLAGS_op_type, ".csv");
        std::string path = absl::StrCat(FLAGS_cached_dir, "/", filename);
        bool cached = false;
        if (!FLAGS_cached_dir.empty()) {
            if (boost::filesystem::exists(path)) {
                LOG(INFO) << "Re-use previously cached results: " << path;
                correct_output.ReadFromFile(path, "key", "value", "ts", false);
                cached = true;
            }
        }
        if (!cached) {
            SingleThreadExecutor single_thread_executor(&join_configuration, ds.get(), &correct_output);
            single_thread_executor.Run();
            single_thread_executor.Join();
            if (!FLAGS_cached_dir.empty()) {
                std::ofstream of(path);
                of << "key,ts,value" << std::endl;
                for (auto ele : correct_output.elements()) {
                    of << ele->key() << "," << ele->ts() << "," << ele->value() << std::endl;
                }
            }
        }
        LOG(INFO) << "Accuracy is: "
                  << std::to_string(CalculateAccuracy(correct_output.elements(), merged_output.elements()));
    }
    return 0;
}
