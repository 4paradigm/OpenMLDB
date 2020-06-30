/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * status.h
 *
 * Author: chenjing
 * Date: 2019/11/21
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BASE_FE_STATUS_H_
#define SRC_BASE_FE_STATUS_H_
#include <string>
#include "glog/logging.h"
#include "proto/fe_common.pb.h"
#include "proto/fe_type.pb.h"

namespace fesql {
namespace base {

template <typename STREAM, typename... Args>
std::initializer_list<int> __output_literal_args(STREAM& stream,  // NOLINT
                                                 Args... args) {  // NOLINT
    return std::initializer_list<int>{(stream << args, 0)...};
}

#define CHECK_STATUS(call, ...)                                               \
    while (true) {                                                            \
        auto _status = (call);                                                \
        if (!_status.isOK()) {                                                \
            fesql::base::__output_literal_args(                               \
                LOG(WARNING), "Internal api error: ", ##__VA_ARGS__, " (at ", \
                __FILE__, ":", __LINE__, ")");                                \
            return _status;                                                   \
        }                                                                     \
        break;                                                                \
    }

#define CHECK_TRUE(call, ...)                                                 \
    while (true) {                                                            \
        if (!(call)) {                                                        \
            std::stringstream _ss;                                            \
            fesql::base::__output_literal_args(                               \
                _ss, "Internal api error: ", ##__VA_ARGS__, "(at ", __FILE__, \
                ":", __LINE__, ")");                                          \
            fesql::base::Status _status(common::kCodegenError, _ss.str());    \
            return _status;                                                   \
        }                                                                     \
        break;                                                                \
    }

struct Status {
    Status() : code(common::kOk), msg("ok") {}
    Status(common::StatusCode status_code, const std::string& msg_str)
        : code(status_code), msg(msg_str) {}

    static Status OK() { return Status(); }

    inline bool isOK() const { return code == common::kOk; }

    common::StatusCode code;
    std::string msg;
};
}  // namespace base
}  // namespace fesql
#endif  // SRC_BASE_FE_STATUS_H_
