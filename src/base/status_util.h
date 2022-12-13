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
#ifndef SRC_BASE_STATUS_UTIL_H_
#define SRC_BASE_STATUS_UTIL_H_

#include "glog/logging.h"
#include "sdk/base.h"

// ref kudu
//
// GCC can be told that a certain branch is not likely to be taken (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absence of better information (ie. -fprofile-arcs).
//
#ifndef PREDICT_FALSE
#if defined(__GNUC__)
#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#else
#define PREDICT_FALSE(x) x
#endif
#endif
#ifndef PREDICT_TRUE
#if defined(__GNUC__)
#define PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#else
#define PREDICT_TRUE(x) x
#endif
#endif

/// @brief Return the given status if it is not @c OK.
#define RETURN_NOT_OK(s)                          \
    do {                                          \
        const ::hybridse::sdk::Status& _s = (s);  \
        if (PREDICT_FALSE(!_s.IsOK())) return _s; \
    } while (0)

/// @brief Return the given status if it is not OK, but first clone it and
///   prepend the given message.
#define RETURN_NOT_OK_PREPEND(s, msg)                                  \
    do {                                                               \
        const ::hybridse::sdk::Status& _s = (s);                       \
        if (PREDICT_FALSE(!_s.IsOK())) return _s.CloneAndPrepend(msg); \
    } while (0)

/// @brief Return @c to_return if @c to_call returns a bad status.
///   The substitution for 'to_return' may reference the variable
///   @c s for the bad status.
#define RETURN_NOT_OK_RET(to_call, to_return)             \
    do {                                                  \
        const ::hybridse::sdk::Status& s = (to_call);     \
        if (PREDICT_FALSE(!s.IsOK())) return (to_return); \
    } while (0)

/// @brief Return the given status if it is not OK, evaluating `on_error` if so.
#define RETURN_NOT_OK_EVAL(s, on_error)          \
    do {                                         \
        const ::hybridse::sdk::Status& _s = (s); \
        if (PREDICT_FALSE(!_s.IsOK())) {         \
            (on_error);                          \
            return _s;                           \
        }                                        \
    } while (0)

/// @brief Emit a warning if @c to_call returns a bad status.
#define WARN_NOT_OK(to_call, warning_prefix)                    \
    do {                                                        \
        const ::hybridse::sdk::Status& _s = (to_call);          \
        if (PREDICT_FALSE(!_s.IsOK())) {                        \
            LOG(WARNING) << (warning_prefix) << ": " << _s.msg; \
        }                                                       \
    } while (0)

/// @brief Log the given status and return immediately.
#define LOG_AND_RETURN(level, status)                 \
    do {                                              \
        const ::hybridse::sdk::Status& _s = (status); \
        LOG(level) << _s.msg;                         \
        return _s;                                    \
    } while (0)

/// @brief If the given status is not OK, log it and 'msg' at 'level' and return the status.
#define RETURN_NOT_OK_LOG(s, level, msg)                        \
    do {                                                        \
        const ::hybridse::sdk::Status& _s = (s);                \
        if (PREDICT_FALSE(!_s.IsOK())) {                        \
            LOG(level) << "Status: " << _s.msg << " " << (msg); \
            return _s;                                          \
        }                                                       \
    } while (0)

/// @brief Set code and msg, and log it at warning. Must be not ok, skip check code
#define SET_STATUS_AND_WARN(s, code, msg)             \
    do {                                              \
        ::hybridse::sdk::Status* _s = (s);            \
        _s->SetCode(code);                            \
        _s->SetMsg((msg));                            \
        LOG(WARNING) << "Status: " << _s->ToString(); \
    } while (0)

/// @brief Set sdk::Status to rpc error, msg is brpc cntl and rpc response, and warn it
#define RPC_STATUS_AND_WARN(s, cntl, resp, pre_msg)             \
    do {                                                        \
        ::hybridse::sdk::Status* _s = (s);                      \
        _s->SetCode(::hybridse::common::StatusCode::kRpcError); \
        _s->SetMsg((pre_msg));                                  \
        _s->Append((cntl->ErrorText().c_str()));                \
        _s->Append((resp->msg()));                              \
        LOG(WARNING) << "Status: " << _s->ToString();           \
    } while (0)

/// @brief Set sdk::Status to server error, msg is openmldb::base::ReturnCode+msg. Must be not ok, skip check code and
/// warn it
#define APPEND_FROM_BASE_AND_WARN(s, base_s, msg)                  \
    do {                                                           \
        ::hybridse::sdk::Status* _s = (s);                         \
        _s->SetCode(::hybridse::common::StatusCode::kServerError); \
        _s->SetMsg((msg));                                         \
        _s->Append((base_s.GetCode()));                            \
        _s->Append((base_s.GetMsg()));                             \
        LOG(WARNING) << "Status: " << _s->ToString();              \
    } while (0)

#define APPEND_FROM_BASE(s, base_s, msg)                           \
    do {                                                           \
        ::hybridse::sdk::Status* _s = (s);                         \
        _s->SetCode(::hybridse::common::StatusCode::kServerError); \
        _s->SetMsg((msg));                                         \
        _s->Append((base_s.GetCode()));                            \
        _s->Append((base_s.GetMsg()));                             \
    } while (0)

/// @brief s.msg = s.msg + prepend_str, and warn it
///   For some funcs only setting status msg without setting code
#define CODE_PREPEND_AND_WARN(s, code, msg)           \
    do {                                              \
        ::hybridse::sdk::Status* _s = (s);            \
        _s->SetCode(code);                            \
        _s->Prepend((msg));                           \
        LOG(WARNING) << "Status: " << _s->ToString(); \
    } while (0)

/// @brief s.msg += append_str, and warn it
#define CODE_APPEND_AND_WARN(s, code, msg)            \
    do {                                              \
        ::hybridse::sdk::Status* _s = (s);            \
        _s->SetCode(code);                            \
        _s->Append((msg));                            \
        LOG(WARNING) << "Status: " << _s->ToString(); \
    } while (0)

#define RET_FALSE_IF_NULL_AND_WARN(call, msg) \
    if ((call) == nullptr) {                  \
        LOG(WARNING) << (msg);                \
        return false;                         \
    }

#define RET_IF_NULL_AND_WARN(call, msg) \
    if ((call) == nullptr) {            \
        LOG(WARNING) << (msg);          \
        return {};                      \
    }

/// @brief Copy code and msg from @c hbs: hybidse::base::Status, and prepend msg. And warn it.
///   Impl: set prepend msg and append @c hbs msg
#define COPY_PREPEND_AND_WARN(s, hbs, msg)            \
    do {                                              \
        ::hybridse::sdk::Status* _s = (s);            \
        _s->SetCode((hbs.GetCode()));                 \
        _s->SetMsg((msg));                            \
        _s->Append((hbs.GetMsg()));                   \
        _s->SetTraces((hbs.GetTraces()));             \
        LOG(WARNING) << "Status: " << _s->ToString(); \
    } while (0)

/// @brief Return @c to_return if @c to_call returns a bad status.
///   The substitution for 'to_return' may reference the variable
///   @c s for the bad status.
#define WARN_NOT_OK_AND_RET(to_call, warning_prefix, to_return)         \
    do {                                                                \
        ::hybridse::sdk::Status* _s = (to_call);                        \
        if (PREDICT_FALSE(!_s->IsOK())) {                               \
            LOG(WARNING) << (warning_prefix) << "--" << _s->ToString(); \
            return (to_return);                                         \
        }                                                               \
    } while (0)

/// @brief If @c to_call returns a bad status, CHECK immediately with
///   a logged message of @c msg followed by the status.
#define CHECK_OK_PREPEND(to_call, msg)                 \
    do {                                               \
        const ::hybridse::sdk::Status& _s = (to_call); \
        CHECK(_s.IsOK()) << (msg) << ": " << _s.msg;   \
    } while (0)

/// @brief If the status is bad, CHECK immediately, appending the status to the
///   logged message.
#define CHECK_OK(s) CHECK_OK_PREPEND(s, "Bad status")

/// @brief If @c to_call returns a bad status, DCHECK immediately with
///   a logged message of @c msg followed by the status.
#define DCHECK_OK_PREPEND(to_call, msg)                \
    do {                                               \
        const ::hybridse::sdk::Status& _s = (to_call); \
        DCHECK(_s.IsOK()) << (msg) << ": " << _s.msg;  \
    } while (0)

/// @brief If the status is bad, DCHECK immediately, appending the status to the
///   logged 'Bad status' message.
#define DCHECK_OK(s) DCHECK_OK_PREPEND(s, "Bad status")

/// @brief A macro to use at the main() function level if it's necessary to
///   return a non-zero status from the main() based on the non-OK status 's'
///   and extra message 'msg' prepended. The desired return code is passed as
///   'ret_code' parameter.
#define RETURN_MAIN_NOT_OK(to_call, msg, ret_code)                          \
    do {                                                                    \
        DCHECK_NE(0, (ret_code)) << "non-OK return code should not be 0";   \
        const ::hybridse::sdk::Status& _s = (to_call);                      \
        if (!_s.IsOK()) {                                                   \
            const ::hybridse::sdk::Status& _ss = _s.CloneAndPrepend((msg)); \
            LOG(ERROR) << _ss.msg;                                          \
            return (ret_code);                                              \
        }                                                                   \
    } while (0)

namespace openmldb::base {}  // namespace openmldb::base

#endif  // SRC_BASE_STATUS_UTIL_H_
