/*
 * op.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_VM_OP_H_
#define SRC_VM_OP_H_

#include "proto/type.pb.h"

namespace fesql {
namespace vm {

enum OpType {
    kOpLoop = 1
};



struct LoopOp {
    uint32_t tid;
    uint32_t pid;
    int8_t* projection_fn;
    std::vector<::fesql::type::ColumnDef> schema;
    uint32_t output_size;
    OpType type;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_OP_H_
