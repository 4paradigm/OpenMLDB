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

#include "common/process_function.h"

#include <gflags/gflags.h>

DEFINE_int32(duplicate_checking_slot, 10, "number of slots used for duplicate checking");

namespace streaming {
namespace interval_join {

ProcessFunction::ProcessFunction(const Element* base_ele)
    :
#ifdef SLOT_DUPLICATE_CHECKING
      recent_probes_(FLAGS_duplicate_checking_slot),
      size_(FLAGS_duplicate_checking_slot),
#endif
      base_ele_(base_ele) {
}

void ProcessFunction::AddElementAtomic(const Element* probe_ele) {
    DCHECK(ElementType::kProbe == probe_ele->type());
    {
#ifdef SLOT_DUPLICATE_CHECKING
        auto idx = probe_ele->id() % size_;
        auto& curr_ele = recent_probes_[idx];
        Element* old_val = curr_ele.load(std::memory_order_relaxed);
        if (old_val == probe_ele) {
            return;
        }
        atomic_store_explicit(&curr_ele, probe_ele, std::memory_order_relaxed);
#elif defined(LOCK_DUCPLICATE_CHECKING)
        std::lock_guard lock(mutex_);
        if (recent_probes_.count(probe_ele)) {
            return;
        }
        recent_probes_.insert(probe_ele);
#endif
    }
    AddElementAtomicInternal(probe_ele);
}

}  // namespace interval_join
}  // namespace streaming
