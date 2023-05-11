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

#ifndef SRC_BASE_RESPONSE_UTIL_H_
#define SRC_BASE_RESPONSE_UTIL_H_

/// @brief
#define SET_RESP_AND_WARN(s, code, msg_str)          \
    do {                                         \
        auto* _s = (s);                          \
        _s->set_code((code));                    \
        _s->set_msg((msg_str));                      \
        LOG(WARNING) << "Set response: " << _s->msg(); \
    } while (0)

#endif  // SRC_BASE_RESPONSE_UTIL_H_
