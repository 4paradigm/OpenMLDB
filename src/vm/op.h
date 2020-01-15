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

#include <storage/window.h>
#include <string>
#include <utility>
#include <vector>
#include <set>
#include "proto/type.pb.h"

namespace fesql {
namespace vm {

enum OpType {
    kOpProject = 1,
    kOpScan,
    kOpLimit,
    kOpMerge
};

struct OpNode {
    virtual ~OpNode() {}
    OpType type;
    uint32_t idx;
    std::vector<::fesql::type::ColumnDef> output_schema;
    std::vector<OpNode*> children;
};

struct ScanOp : public OpNode {
    ~ScanOp() {}
    std::string db;
    uint32_t tid;
    uint32_t pid;
    uint32_t limit;
    std::vector<::fesql::type::ColumnDef> input_schema;
};

// TODO(chenjing): WindowOp
struct ScanInfo {
    std::set<std::pair<fesql::type::Type, uint32_t >> keys;
    std::pair<fesql::type::Type, uint32_t > order;
    std::string index_name;
    // todo(chenjing): start and end parse
    int64_t start_offset;
    int64_t end_offset;
    bool has_order;
    bool is_range_between;
};

struct ProjectOp : public OpNode {
    ~ProjectOp() {}
    std::string db;
    uint32_t tid;
    uint32_t pid;
    uint32_t scan_limit;
    int8_t* fn;
    std::string fn_name;
    bool window_agg;
    ScanInfo w;
};

struct LimitOp : public OpNode {
    ~LimitOp() {}
    uint32_t limit;
};

struct MergeOp : public OpNode {
    int8_t* fn;
    std::vector<std::pair<uint32_t , uint32_t >> pos_mapping;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_OP_H_
