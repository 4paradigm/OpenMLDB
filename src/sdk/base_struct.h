/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * base_struct.h
 *
 * Author: chenjing
 * Date: 2019/11/21
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_SDK_BASE_STRUCT_H
#define SRC_SDK_BASE_STRUCT_H
#include <string>
namespace fesql {
namespace sdk {

struct Status {
    Status() : code(0), msg("ok") {}
    Status(int status_code, const std::string &msg_str)
        : code(status_code), msg(msg_str) {}
    int code;
    std::string msg;
};

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_BASE_STRUCT_H
