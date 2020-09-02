/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * fe_status.cc
 *
 * Author: chenjing
 * Date: 2019/11/21
 *--------------------------------------------------------------------------
 **/
#include "base/fe_status.h"

namespace fesql {
namespace base {

std::ostream& operator<<(std::ostream& os, const Status& status) {  // NOLINT
    os << status.msg << "\n" << status.trace;
    return os;
}

}  // namespace base
}  // namespace fesql
