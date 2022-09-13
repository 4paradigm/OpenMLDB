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

#ifndef STREAM_SRC_COMMON_CLEAN_INFO_H_
#define STREAM_SRC_COMMON_CLEAN_INFO_H_

#include <string>

namespace streaming {
namespace interval_join {
class CleanInfo {
 public:
    CleanInfo(int64_t clean_ts, const std::string& key, int64_t ts, bool is_base)
    : clean_ts_(clean_ts), key_(key), ts_(ts), is_base_(is_base) {}

    int64_t clean_ts() const { return clean_ts_; }
    const std::string& key() const {return key_; }
    int64_t ts() const {return ts_; }
    bool is_base() const {return is_base_; }

 private:
    int64_t clean_ts_;
    std::string key_;
    int64_t ts_;
    bool is_base_;
};

class CleanInfoCompare {
 public:
    bool operator() (const CleanInfo& one, const CleanInfo& two) {
        return one.clean_ts() > two.clean_ts();
    }
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_COMMON_CLEAN_INFO_H_
