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

#ifndef STREAM_SRC_COMMON_PROCESS_FUNCTION_H_
#define STREAM_SRC_COMMON_PROCESS_FUNCTION_H_

#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "common/element.h"
#include "common/function.h"

namespace streaming {
namespace interval_join {

// #define SLOT_DUPLICATE_CHECKING

enum class OpType {
    Count,
    DistinctCount,
    Sum,
    Unknown
};

class ProcessFunction : public Function {
 public:
    explicit ProcessFunction(const Element* base_ele);
    ~ProcessFunction() override {}

    virtual std::shared_ptr<ProcessFunction> Clone() const {
        LOG(ERROR) << "Not support clone";
        return nullptr;
    }

    void SetBase(const Element* base_ele) {
        base_ele_ = base_ele;
    }

    inline void set_start_ts(int64_t start_ts) {
        start_ts_ = start_ts;
    }

    inline int64_t start_ts() const {
        return start_ts_;
    }


    int64_t GetTs() const { return base_ele_->ts(); }
    virtual void AddElement(Element* probe_ele) = 0;
    virtual std::string GetResult() = 0;

    void AddElementAtomic(const Element* probe_ele);
    virtual std::string GetResultAtomic() = 0;

    virtual void RemoveElement(const Element* probe_ele) {
        LOG(ERROR) << "Not support RemoveElement";
    }

    virtual void Reset() {
        start_ts_ = 0;
    }

 protected:
    virtual void AddElementAtomicInternal(const Element* probe_ele) = 0;

    const Element* base_ele_ = nullptr;
    int64_t start_ts_ = 0;
#ifdef SLOT_DUPLICATE_CHECKING
    std::vector<std::atomic<Element*>> recent_probes_;
    size_t size_;
#else
    std::set<Element*> recent_probes_;
    std::mutex mutex_;
#endif
};

}  // namespace interval_join
}  // namespace streaming


#endif  // STREAM_SRC_COMMON_PROCESS_FUNCTION_H_
