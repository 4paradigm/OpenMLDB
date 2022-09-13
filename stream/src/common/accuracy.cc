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
#include <unordered_map>
#include <vector>
#include <utility>
#include <string>

#include "common/accuracy.h"

namespace streaming {
namespace interval_join {

DEFINE_bool(print_wrong_res, false, "whether print out the wrong results");

double CalculateAccuracy(const std::vector<std::shared_ptr<Element>>& correct_res,
                         const std::vector<std::shared_ptr<Element>>& res) {
    std::unordered_map<std::string, std::unordered_map<int64_t, std::vector<Element*>>> join_result;

    for (auto& ele : res) {
        Element* result_ele = ele.get();
        const std::string& key = result_ele->key();
        auto it = join_result.find(key);
        if (it == join_result.end()) {
            it = join_result.emplace(std::make_pair(key, std::unordered_map<int64_t, std::vector<Element*>>())).first;
        }
        auto& keyed_map = it->second;
        // overwrite if there is existing element --> only keep the latest result element
        keyed_map[result_ele->ts()].push_back(result_ele);
    }

    double correct_num = 0;
    for (auto& ele : correct_res) {
        Element* correct_ele = ele.get();
        const std::string& key = correct_ele->key();
        int64_t ts = correct_ele->ts();
        auto keyed_it = join_result.find(key);
        if (keyed_it == join_result.end()) {
            // no matching key
            continue;
        }
        auto& keyed_map = keyed_it->second;
        auto ele_it = keyed_map.find(ts);
        if (ele_it == keyed_map.end()) {
            // no matching timestamp
            continue;
        }
        auto result_eles = ele_it->second;
        bool correct = false;
        for (auto result_ele : result_eles) {
            if (correct_ele->value() == result_ele->value()) {
                // element with same values
                correct_num++;
                correct = true;
                break;
            }
        }
        if (!correct && FLAGS_print_wrong_res) {
            LOG(WARNING) << ele->ToString() << " Not correct";
            for (auto result_ele : result_eles) {
                LOG(WARNING) << "res: " << result_ele->ToString();
            }
        }
    }
    return correct_num / correct_res.size();
}

}  // namespace interval_join
}  // namespace streaming

