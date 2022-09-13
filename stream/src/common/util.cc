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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <ctime>
#include <algorithm>
#include <fstream>
#include <limits>
#include <vector>
#include <utility>
#include <sstream>

#include "common/util.h"

namespace streaming {
namespace interval_join {

DEFINE_int64(latency_unit, 1000, "latency unit");

double GetLatency(DataStream* ds, const std::string& path) {
    auto& eles = ds->elements();
    double latency = 0;
    std::vector<int64_t> latency_recorder;
    int64_t max_latency = -1;
    for (auto& ele : eles) {
        CHECK_NE(-1, ele->output_time());
        CHECK_NE(-1, ele->read_time());
        CHECK_LE(ele->read_time(), ele->output_time());
        int64_t ele_latency = (ele->output_time() - ele->read_time());
        max_latency = std::max(ele_latency, max_latency);
        latency = latency + ele_latency;
    }

    latency_recorder.resize((max_latency + FLAGS_latency_unit - 1) / FLAGS_latency_unit);

    // scan and count elements' latency
    for (auto& ele : eles) {
        // purpose of -1: to count all elements which has equal or smaller latency for each level
        int64_t latency_level = (ele->output_time() - ele->read_time()) / FLAGS_latency_unit;
        latency_recorder[latency_level]++;
    }

    // get accumulative counts
    for (int i = 1; i < latency_recorder.size(); i++) {
        latency_recorder[i] = latency_recorder[i] + latency_recorder[i - 1];
    }

    CHECK_EQ(latency_recorder.back(), eles.size());

    // output latency results into file
    std::ofstream output_file(path, std::ios_base::out);
    if (output_file.is_open()) {
        output_file << "latency,count" << std::endl;
        output_file << "0,0" << std::endl;
        for (int i = 0; i < latency_recorder.size(); i++) {
            output_file << (i + 1) * FLAGS_latency_unit << "," << latency_recorder[i] << std::endl;
        }
        output_file << std::numeric_limits<int64_t>::max() << "," <<  eles.size() << std::endl;
        output_file.close();
    } else {
        LOG(INFO) << "cannot open latency output file";
        return -1;
    }

    return latency / eles.size();
}

double GetJoinCounts(DataStream* ds) {
    auto& eles = ds->elements();
    double res = 0;
    double count = 0;
    for (auto& ele : eles) {
        if (ele->type() == ElementType::kProbe) {
            count++;
            res += ele->join_count();
        }
    }
    if (count == 0) {  // no probe elements
        return 0;
    }
    return res / count;
}

double GetBaseEffectiveness(DataStream* ds) {
    auto& eles = ds->elements();
    double res = 0;
    double count = 0;
    for (auto& ele : eles) {
        if (ele->type() == ElementType::kBase && ele->effectiveness() != -1) {
            res += ele->effectiveness();
            count++;
        }
    }
    return res / count;
}

double GetProbeEffectiveness(DataStream* ds) {
    auto& eles = ds->elements();
    double res = 0;
    double count = 0;
    for (auto& ele : eles) {
        if (ele->type() == ElementType::kProbe && ele->effectiveness() != -1) {
            res += ele->effectiveness();
            count++;
        }
    }
    return res / count;
}

}  // namespace interval_join
}  // namespace streaming
