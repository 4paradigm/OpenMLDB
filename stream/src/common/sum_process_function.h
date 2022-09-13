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

#include <glog/logging.h>

#include <memory>
#include <string>

#include "common/process_function.h"

#ifndef STREAM_SRC_COMMON_SUM_PROCESS_FUNCTION_H_
#define STREAM_SRC_COMMON_SUM_PROCESS_FUNCTION_H_

namespace streaming {
namespace interval_join {
class SumProcessFunction : public ProcessFunction {
 public:
    explicit SumProcessFunction(const Element* base_ele) : ProcessFunction(base_ele) {}
    ~SumProcessFunction() override {}
    std::string GetResult() override { return std::to_string(sum_); }
    void AddElement(Element* probe_ele) override {
        try {
            sum_ = sum_ + std::stod(probe_ele->value());
        } catch(const std::invalid_argument & e) {
            LOG(ERROR) << "The input cannot be converted to number";
        } catch (const std::out_of_range & e) {
            LOG(ERROR) << "The input exceeds the range of long long";
        }
    }

    void AddElementAtomicInternal(const Element* probe_ele) override {
        try {
            double old_sum;
            double val = std::stod(probe_ele->value());
            double new_sum = 0;
            do {
                double old_sum = sum_.load(std::memory_order_relaxed);
                new_sum = old_sum + val;
            } while (!std::atomic_compare_exchange_weak(&sum_, &old_sum, new_sum));
        } catch(const std::invalid_argument & e) {
            LOG(ERROR) << "The input cannot be converted to number";
        } catch (const std::out_of_range & e) {
            LOG(ERROR) << "The input exceeds the range of long long";
        }
    }

    std::string GetResultAtomic() override {
        return GetResult();;
    }

    std::shared_ptr<ProcessFunction> Clone() const override {
        auto cloned = std::make_shared<SumProcessFunction>(base_ele_);
        // FIXME(zhanghao): thread-safe
        cloned->sum_ = sum_.load();
        return cloned;
    }

    void RemoveElement(const Element* probe_ele) override {
        double val = std::stod(probe_ele->value());
        sum_ = sum_ - val;
    }

    void Reset() override {
        ProcessFunction::Reset();
        sum_.store(0);
    }

 private:
    std::atomic<double> sum_ = 0;
};

}  // namespace interval_join
}  // namespace streaming
#endif  // STREAM_SRC_COMMON_SUM_PROCESS_FUNCTION_H_
