/*
 * fe_status.cc
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
