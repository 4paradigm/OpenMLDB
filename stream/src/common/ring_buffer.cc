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

#include "common/ring_buffer.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>

namespace streaming {
namespace interval_join {

Element* RingBuffer::GetUnblocking() {
    if (Avail()) {
        Element* ele;
        auto pos = FetchAndIncrConsumerPos();
        do {
            ele = ring_.at(pos).load();
        } while (ele == nullptr);

        auto& ele_ref = ring_.at(pos);
        std::atomic_compare_exchange_weak(&ele_ref, &ele, (Element*)nullptr);  // NOLINT
        return ele;
    } else {
        return nullptr;
    }
}

bool RingBuffer::PutUnblocking(Element* ele) {
    auto slot = GetFreeSlot();
    if (slot != -1) {
        ring_.at(slot) = ele;
        return true;
    } else {
        return false;
    }
}

}  // namespace interval_join
}  // namespace streaming
