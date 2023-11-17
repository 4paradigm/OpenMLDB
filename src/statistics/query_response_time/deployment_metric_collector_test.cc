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

#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>

#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace bvar {
DECLARE_int32(bvar_dump_interval);
}

namespace openmldb {
namespace statistics {

class CollectorTest : public ::testing::Test {
 public:
    CollectorTest() = default;
    ~CollectorTest() override = default;
};

using std::ifstream;
using std::string;
using std::ios_base;

void mem_usage() {
    double vm_usage = 0.0;
    double resident_set = 0.0;
    ifstream stat_stream("/proc/self/stat", ios_base::in);  // get info from proc directory
    // create some variables to get info
    string pid, comm, state, ppid, pgrp, session, tty_nr;
    string tpgid, flags, minflt, cminflt, majflt, cmajflt;
    string utime, stime, cutime, cstime, priority, nice;
    string O, itrealvalue, starttime;
    uint64_t vsize;
    int64_t rss;
    stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid >> flags >> minflt >> cminflt >>
        majflt >> cmajflt >> utime >> stime >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >> starttime >>
        vsize >> rss;  // don't care about the rest
    stat_stream.close();
    int64_t page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024;  // for x86-64 is configured to use 2MB pages
    vm_usage = vsize / 1024.0;
    resident_set = rss * page_size_kb;
    LOG(INFO) << "VM: " << vm_usage << "KB; RSS: " << resident_set << "KB";
}

TEST_F(CollectorTest, MemoryTest) {
    // let's see how much memory will be used
    auto test = [](int db_size, int dpl_size, int request_cnt, bool will_fail = false) {
        DeploymentMetricCollector collector("test");
        for (int db_idx = 0; db_idx < db_size; db_idx++) {
            auto db = "db" + std::to_string(db_idx);
            for (int i = 0; i < dpl_size; i++) {
                for (int t = 0; t < request_cnt; t++) {
                    auto st = collector.Collect(db, "d" + std::to_string(i), absl::Microseconds(10));
                }
            }
        }
        if (will_fail) {
            return;
        }
        auto desc = collector.Desc({});
        LOG(INFO) << desc;
        ASSERT_TRUE(desc ==
                    absl::StrCat(R"({"name" : "test_deployment", "labels" : ["db", "deployment"], "stats_count" : )",
                                 db_size * dpl_size, "}"))
            << desc;
        // peek one
        auto dstat = collector.Desc({"db0", "d0"});
        LOG(INFO) << dstat;
        // can't check qps, and latency is calc from window, it'll == 0 if get too fase, so we just check count
        ASSERT_TRUE(dstat.find("count:" + std::to_string(request_cnt)) != std::string::npos) << dstat;
        mem_usage();
    };
    // empty mem VM: 104948KB; RSS: 5752KB
    mem_usage();
    // recorder mem for one label is stable, VM: ~105576KB; RSS: ~6512KB, even collect 10M requests
    // but VM&RSS containes other memory, should use diff to get recorder mem
    test(1, 1, 1000);  // + ~0.5M
    test(1, 1, 10000);
    test(1, 1, 100000);
    // disable for speed
    // test(1, 1, 1000000);
    // test(1, 1, 10000000);

    // VM: 105568KB; RSS: 6628KB
    // VM: 105964KB; RSS: 6984KB
    // VM: 110056KB; RSS: 11208KB
    // VM: 341356KB; RSS: 126256KB
    // VM: 344076KB; RSS: 129936KB
    // test(1, 10, 1000);
    // test(1, 100, 1000);
    // test(1, 1000, 1000);
    // test(1, 10000, 1000);
    // test(10, 1000, 1000);

    // MAX_MULTI_DIMENSION_STATS_COUNT = 20000, so total alive deployment count <= 20001
    // If we need more, need a repetitive task to reset (K*bvar_bump_interval?) or add LabelLatencyRecorder
    test(1, 20002, 1, true);
}

}  // namespace statistics
}  // namespace openmldb

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    bvar::FLAGS_bvar_dump_interval = 75;
    return RUN_ALL_TESTS();
}
