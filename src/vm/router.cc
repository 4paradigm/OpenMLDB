/*
 * sql_compiler.cc
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

#include "vm/router.h"
#include "node/sql_node.h"
namespace fesql {
namespace vm {

int Router::Parse(const PhysicalOpNode* physical_plan) {
	if (physical_plan == nullptr) {
		return -1;
	}
    if (physical_plan->GetOpType() == kPhysicalOpRequestUnion) {
        auto request_union_node = dynamic_cast<const PhysicalRequestUnionNode*>(physical_plan);
        if (request_union_node) {
            auto keys = request_union_node->window().partition().keys();
            if (keys != nullptr && keys->GetChildNum() > 0) {
                auto exp_node = keys->GetChild(0);
                auto columnNode = dynamic_cast<fesql::node::ColumnRefNode*>(exp_node);
                if (columnNode != nullptr) {
                    router_col_ = columnNode->GetColumnName();
                    return 0;
                }
            }
        }
    }
    for (auto productor : physical_plan->GetProducers()) {
        if (Parse(productor) == 0) {
            return 0;
        }
    }
    return 1;
}

}
}

