/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_BASE_PROTO_UTIL_H_
#define SRC_BASE_PROTO_UTIL_H_
#include <string>

#include "base/status.h"

namespace openmldb {
namespace base {

template <typename Response>
void SetResponseStatus(int code, const char* msg, Response* response) {
    if (msg != nullptr && response != nullptr) {
        response->set_code(code);
        response->set_msg(msg);
    }
}

template <typename Response>
void SetResponseStatus(int code, const std::string& msg, Response* response) {
    if (response != nullptr) {
        response->set_code(code);
        response->set_msg(msg);
    }
}

/// @brief Set code and msg, and log it at warning. Must be not ok, skip check code
#define SET_RESP_AND_WARN(s, c, m)                                         \
    do {                                                                   \
        (s)->set_code(static_cast<int>(c));                                \
        (s)->set_msg((m));                                                 \
        LOG(WARNING) << "Set resp: " << (s)->code() << ", " << (s)->msg(); \
    } while (0)

template <typename Response>
void SetResponseStatus(const Status& status, Response* response) {
    if (response != nullptr) {
        response->set_code(status.code);
        response->set_msg(status.msg);
    }
}

template <typename Response>
void SetResponseOK(Response* response) {
    if (response != nullptr) {
        response->set_code(ReturnCode::kOk);
        response->set_msg("ok");
    }
}

}  // namespace base
}  // namespace openmldb
#endif  // SRC_BASE_PROTO_UTIL_H_
