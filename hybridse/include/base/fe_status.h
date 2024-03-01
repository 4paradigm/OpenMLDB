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

#ifndef HYBRIDSE_INCLUDE_BASE_FE_STATUS_H_
#define HYBRIDSE_INCLUDE_BASE_FE_STATUS_H_

#include <sstream>
#include <string>
#include <vector>

#include "proto/fe_common.pb.h"

namespace hybridse {
namespace base {

template <typename STREAM, typename... Args>
static inline std::initializer_list<int> __output_literal_args(STREAM& stream,  // NOLINT
                                                               Args... args) {  // NOLINT
#ifdef __APPLE__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreturn-stack-address"
#endif
    return std::initializer_list<int>{(stream << args, 0)...};
#ifdef __APPLE__
#pragma GCC diagnostic pop
#endif
}

#define MAX_STATUS_TRACE_SIZE 4096

// Evaluate and check the expression returns a absl::Status.
// End the current function by return status, if status is not OK
#define CHECK_ABSL_STATUS(expr) \
    while (true) {              \
        auto _s = (expr);       \
        if (!_s.ok()) {         \
            return _s;          \
        }                       \
        break;                  \
    }

// Check the absl::StatusOr<T> object, end the current function
// by return 'object.status()' if it is not OK
#define CHECK_ABSL_STATUSOR(statusor) \
    while (true) {                    \
        if (!statusor.ok()) {         \
            return statusor.status(); \
        }                             \
        break;                        \
    }

// Evaluate the expression returns Status, converted and return failed absl status if status not ok
#define CHECK_STATUS_TO_ABSL(expr)                        \
    while (true) {                                        \
        auto _status = (expr);                            \
        if (!_status.isOK()) {                              \
            return absl::InternalError(_status.GetMsg()); \
        }                                                 \
        break;                                            \
    }

#define CHECK_STATUS(call, ...)                                         \
    while (true) {                                                      \
        auto _status = (call);                                          \
        if (!_status.isOK()) {                                          \
            std::stringstream _msg;                                     \
            hybridse::base::__output_literal_args(_msg, ##__VA_ARGS__); \
            _status.AddTrace(__FILE__, __LINE__, _msg.str());           \
            return _status;                                              \
        }                                                               \
        break;                                                          \
    }

#define CHECK_TRUE(call, errcode, ...)                                  \
    while (true) {                                                      \
        if (!(call)) {                                                  \
            std::stringstream _msg;                                     \
            hybridse::base::__output_literal_args(_msg, ##__VA_ARGS__); \
            hybridse::base::Status _status(errcode, _msg.str());        \
            _status.AddTrace(__FILE__, __LINE__, _msg.str());           \
            return _status;                                             \
        }                                                               \
        break;                                                          \
    }
#define FAIL_STATUS(errcode, ...)                                   \
    while (true) {                                                  \
        std::stringstream _msg;                                     \
        hybridse::base::__output_literal_args(_msg, ##__VA_ARGS__); \
                                                                    \
        hybridse::base::Status _status(errcode, _msg.str());        \
        _status.AddTrace(__FILE__, __LINE__, _msg.str());           \
        return _status;                                             \
        break;                                                      \
    }
struct Trace {
    Trace(const std::string& file, const int line, const std::string& msg) : file(file), line(line), msg(msg) {}
    ~Trace() {}
    std::string file;
    int line;
    std::string msg;
};
struct Status {
    Status() : code(common::kOk), msg("ok") {}

    explicit Status(common::StatusCode status_code) : code(status_code), msg("") {}

    Status(common::StatusCode status_code, const std::string& msg_str) : code(status_code), msg(msg_str) {}

    static Status OK() { return Status(); }
    static Status Running() { return Status(common::kRunning, "running"); }

    inline bool isOK() const { return code == common::kOk; }
    inline bool isRunning() const { return code == common::kRunning; }
    const std::string str() const { return GetMsg()+ "\n" + GetTraces(); }
    const common::StatusCode GetCode() const { return code;}
    const std::string GetMsg() const {
        return msg;
    }
    const std::string GetTraces() const {
        std::stringstream trace_msg;
        for (auto iter = traces.rbegin(); iter != traces.rend(); iter++) {
            trace_msg << "    (At " << iter->file << ":" << iter->line << ")\n";
            if (!iter->msg.empty()) {
                trace_msg << "    (Caused by) " << iter->msg << "\n";
            }
        }
        return trace_msg.str();
    }
    void AddTrace(const std::string& file, const int line, const std::string& msg) {
        if (traces.size() >= MAX_STATUS_TRACE_SIZE) {
            traces.pop_back();
        }
        traces.emplace_back(file, line, msg);
    }
    common::StatusCode code;
    std::string msg;
    std::vector<Trace> traces;
};

std::ostream& operator<<(std::ostream& os, const Status& status);  // NOLINT

}  // namespace base
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_BASE_FE_STATUS_H_
