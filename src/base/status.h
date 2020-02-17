/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * status.h
 *
 * Author: chenjing
 * Date: 2019/11/21
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BASE_STATUS_H_
#define SRC_BASE_STATUS_H_
#include <string>
#include "proto/common.pb.h"
#include "proto/type.pb.h"

namespace fesql {
namespace base {
struct Status {
    Status() : code(common::kOk), msg("ok") {}
    Status(common::StatusCode status_code, const std::string &msg_str)
        : code(status_code), msg(msg_str) {}
    common::StatusCode code;
    std::string msg;
};
}  // namespace base
}  // namespace fesql
#endif  // SRC_BASE_STATUS_H_
