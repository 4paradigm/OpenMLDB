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

#include <string>
#include <unordered_set>

#include "common/process_function.h"

#ifndef STREAM_SRC_COMMON_DISTINCT_COUNT_PROCESS_FUNCTION_H_
#define STREAM_SRC_COMMON_DISTINCT_COUNT_PROCESS_FUNCTION_H_

namespace streaming {
namespace interval_join {
class DistinctCountProcessFunction : public ProcessFunction {
 public:
    explicit DistinctCountProcessFunction(const Element* base_ele) : ProcessFunction(base_ele) {}
    ~DistinctCountProcessFunction() override {}
    void AddElement(Element* probe_ele) override { value_record_.insert(probe_ele->value()); }
    std::string GetResult() override { return std::to_string(value_record_.size()); }

    void AddElementAtomicInternal(const Element* probe_ele) override {}

    std::string GetResultAtomic() override {}

 private:
    std::unordered_set<std::string> value_record_;
};

}  // namespace interval_join
}  // namespace streaming
#endif  // STREAM_SRC_COMMON_DISTINCT_COUNT_PROCESS_FUNCTION_H_
