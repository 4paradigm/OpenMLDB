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

#ifndef INCLUDE_BASE_FE_STATUS_H_
#define INCLUDE_BASE_FE_STATUS_H_
#include <string>
#include "glog/logging.h"
#include "proto/fe_common.pb.h"
#include "proto/fe_type.pb.h"

namespace hybridse {
namespace base {

template <typename STREAM, typename... Args>
static inline std::initializer_list<int> __output_literal_args(STREAM& stream,  // NOLINT
                                                               Args... args) {  // NOLINT
    return std::initializer_list<int>{(stream << args, 0)...};
}

#define MAX_STATUS_TRACE_SIZE 4096

#define CHECK_STATUS(call, ...)                                                                                       \
    while (true) {                                                                                                    \
        auto _status = (call);                                                                                        \
        if (!_status.isOK()) {                                                                                        \
            std::stringstream _msg;                                                                                   \
            hybridse::base::__output_literal_args(_msg, ##__VA_ARGS__);                                               \
            std::stringstream _trace;                                                                                 \
            hybridse::base::__output_literal_args(_trace, "    (At ", __FILE__, ":", __LINE__, ")");                  \
            if (_status.trace.size() >= MAX_STATUS_TRACE_SIZE) {                                                      \
                LOG(WARNING) << "Internal error: " << _status.msg << "\n" << _status.trace;                           \
            } else {                                                                                                  \
                if (!_msg.str().empty()) {                                                                            \
                    _trace << "\n"                                                                                    \
                           << "    (Caused by) " << _msg.str();                                                       \
                }                                                                                                     \
                _trace << "\n" << _status.trace;                                                                      \
            }                                                                                                         \
            return hybridse::base::Status(_status.code, _msg.str().empty() ? _status.msg : _msg.str(), _trace.str()); \
        }                                                                                                             \
        break;                                                                                                        \
    }

#define CHECK_TRUE(call, errcode, ...)                                                               \
    while (true) {                                                                                   \
        if (!(call)) {                                                                               \
            std::stringstream _msg;                                                                  \
            hybridse::base::__output_literal_args(_msg, ##__VA_ARGS__);                              \
            std::stringstream _trace;                                                                \
            hybridse::base::__output_literal_args(_trace, "    (At ", __FILE__, ":", __LINE__, ")"); \
                                                                                                     \
            if (!_msg.str().empty()) {                                                               \
                _trace << "\n"                                                                       \
                       << "    (Caused by) " << _msg.str();                                          \
            }                                                                                        \
            hybridse::base::Status _status(errcode, _msg.str(), _trace.str());                       \
            return _status;                                                                          \
        }                                                                                            \
        break;                                                                                       \
    }
#define FAIL_STATUS(errcode, ...)                                                                \
    while (true) {                                                                               \
        std::stringstream _msg;                                                                  \
        hybridse::base::__output_literal_args(_msg, ##__VA_ARGS__);                              \
        std::stringstream _trace;                                                                \
        hybridse::base::__output_literal_args(_trace, "    (At ", __FILE__, ":", __LINE__, ")"); \
                                                                                                 \
        if (!_msg.str().empty()) {                                                               \
            _trace << "\n"                                                                       \
                   << "    (Caused by) " << _msg.str();                                          \
        }                                                                                        \
        hybridse::base::Status _status(errcode, _msg.str(), _trace.str());                       \
        return _status;                                                                          \
        break;                                                                                   \
    }
struct Status {
    Status() : code(common::kOk), msg("ok") {}

    explicit Status(common::StatusCode status_code) : code(status_code), msg("") {}

    Status(common::StatusCode status_code, const std::string& msg_str) : code(status_code), msg(msg_str) {}

    Status(common::StatusCode status_code, const std::string& msg_str, const std::string& trace_str)
        : code(status_code), msg(msg_str), trace(trace_str) {}

    static Status OK() { return Status(); }
    static Status Running() { return Status(common::kRunning, "running"); }

    inline bool isOK() const { return code == common::kOk; }
    inline bool isRunning() const { return code == common::kRunning; }

    const std::string str() const { return msg + "\n" + trace; }

    common::StatusCode code;

    std::string msg;
    std::string trace;
};

std::ostream& operator<<(std::ostream& os, const Status& status);  // NOLINT

}  // namespace base
}  // namespace hybridse
#endif  // INCLUDE_BASE_FE_STATUS_H_
