/*
 * Copyright 2021 4paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "passes/physical/limit_optimized.h"

namespace hybridse {
namespace passes {

bool LimitOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    *output = in;
    if (vm::kPhysicalOpLimit != in->GetOpType()) {
        return false;
    }

    auto limit_op = dynamic_cast<vm::PhysicalLimitNode*>(in);

    if (ApplyLimitCnt(in->producers()[0], limit_op->GetLimitCnt())) {
        limit_op->SetLimitOptimized(true);
        return true;
    } else {
        return false;
    }
}

bool LimitOptimized::ApplyLimitCnt(PhysicalOpNode* node, std::optional<int32_t> limit_cnt) {
    if (vm::kPhysicalOpLimit == node->GetOpType()) {
        auto limit_op = dynamic_cast<vm::PhysicalLimitNode*>(node);
        if (!node->GetLimitCnt().has_value() || limit_op->GetLimitCnt() > limit_cnt) {
            if (limit_op->GetLimitOptimized()) {
                return ApplyLimitCnt(node->producers()[0], limit_cnt);
            } else {
                limit_op->SetLimitCnt(limit_cnt);
            }
        }
        return true;
    }
    if (node->producers().empty()) {
        return false;
    }
    if (node->GetOpType() == vm::kPhysicalOpSimpleProject ||
        node->GetOpType() == vm::kPhysicalOpRename) {
        return false;
    }
    if (node->is_block()) {
        if (!node->GetLimitCnt().has_value() || node->GetLimitCnt() > limit_cnt) {
            node->SetLimitCnt(limit_cnt);
        }
        return true;
    } else {
        if (!ApplyLimitCnt(node->producers()[0], limit_cnt)) {
            if (!node->GetLimitCnt().has_value() || node->GetLimitCnt() > limit_cnt) {
                node->SetLimitCnt(limit_cnt);
                return true;
            }
        } else {
            return true;
        }
    }
    return false;
}
}  // namespace passes
}  // namespace hybridse
