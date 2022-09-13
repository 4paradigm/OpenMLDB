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

#include "common/buffer.h"

#include <glog/logging.h>
#include <unistd.h>

namespace streaming {
namespace interval_join {

DEFINE_int64(buffer_blocking_sleep_us, 20, "microseconds to sleep between each re-try");

Element* Buffer::Get(bool blocking, int64_t timeout) {
    int64_t try_counter = 0;
    int max_retries = timeout / FLAGS_buffer_blocking_sleep_us;
    int done_checking = 0;
    do {
        auto ret = GetUnblocking();
        if (ret != nullptr) return ret;

        if (!blocking) {
            break;
        }

        if (done_) {
            if (done_checking >= 1) {
                break;
            }
            done_checking++;
        }

        if (timeout != -1 && ++try_counter >= max_retries) {
            LOG(WARNING) << "Blocking Get timeout in " << timeout << " us";
            break;
        }
        if (FLAGS_buffer_blocking_sleep_us != 0) {
            usleep(FLAGS_buffer_blocking_sleep_us);
        }
    } while (true);
    return nullptr;
}

bool Buffer::Put(Element* ele, bool blocking, int64_t timeout) {
    int64_t try_counter = 0;
    int max_retries = timeout / FLAGS_buffer_blocking_sleep_us;
    do {
        auto succeed = PutUnblocking(ele);
        if (succeed) return succeed;

        if (timeout != -1 && ++try_counter >= max_retries) {
            LOG(WARNING) << "Blocking Put timeout in " << timeout << " us";
            break;
        }
        usleep(FLAGS_buffer_blocking_sleep_us);
    } while (blocking);
    return false;
}

}  // namespace interval_join
}  // namespace streaming
