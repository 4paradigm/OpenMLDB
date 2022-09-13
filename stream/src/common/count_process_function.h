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

#ifndef STREAM_SRC_COMMON_COUNT_PROCESS_FUNCTION_H_
#define STREAM_SRC_COMMON_COUNT_PROCESS_FUNCTION_H_

#include <memory>
#include <string>

#include "common/process_function.h"

namespace streaming {
namespace interval_join {

class CountProcessFunction : public ProcessFunction {
 public:
    explicit CountProcessFunction(const Element* base_ele) : ProcessFunction(base_ele) {}
    ~CountProcessFunction() override {}
    void AddElement(Element* probe_ele) override { count_++; }
    std::string GetResult() override { return std::to_string(count_); }

    void AddElementAtomicInternal(const Element* probe_ele) override {
        atomic_fetch_add_explicit(&count_, 1L, std::memory_order_relaxed);
    }

    std::string GetResultAtomic() override {
        return std::to_string(std::atomic_load_explicit(&count_, std::memory_order_relaxed));
    }

    std::shared_ptr<ProcessFunction> Clone() const override {
        auto cloned = std::make_shared<CountProcessFunction>(base_ele_);
        // FIXME(zhanghao): thread-safe
        cloned->count_ = count_.load();
        return cloned;
    }

    void RemoveElement(const Element* probe_ele) override {
        count_--;
    }

 private:
    std::atomic<int64_t> count_ = 0;
};

}  // namespace interval_join
}  // namespace streaming
#endif  // STREAM_SRC_COMMON_COUNT_PROCESS_FUNCTION_H_
