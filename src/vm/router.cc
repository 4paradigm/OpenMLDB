/*
 * Copyright 2021 4Paradigm
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
#include "glog/logging.h"
#include "node/sql_node.h"
namespace fesql {
namespace vm {

bool Router::IsWindowNode(const PhysicalOpNode* physical_node) {
    if (physical_node == nullptr) {
        LOG(WARNING) << "node is null";
        return false;
    }
    if (physical_node->GetOpType() == kPhysicalOpRequestUnion) {
        if (physical_node->GetProducerCnt() > 0) {
            auto node = physical_node->GetProducer(0);
            if (node != nullptr &&
                node->GetOpType() == kPhysicalOpDataProvider) {
                auto provider_node =
                    dynamic_cast<PhysicalDataProviderNode*>(node);
                if (provider_node != nullptr &&
                    provider_node->provider_type_ == kProviderTypeRequest) {
                    return true;
                }
            }
        }
    }
    return false;
}

int Router::Parse(const PhysicalOpNode* physical_plan) {
    if (physical_plan == nullptr) {
        LOG(WARNING) << "node is null";
        return -1;
    }
    if (IsWindowNode(physical_plan)) {
        auto request_union_node =
            dynamic_cast<const PhysicalRequestUnionNode*>(physical_plan);
        if (request_union_node) {
            // auto keys = request_union_node->window().partition().keys();
            auto keys = request_union_node->window().index_key().keys();
            if (keys != nullptr && keys->GetChildNum() > 0) {
                auto exp_node = keys->GetChild(0);
                auto columnNode =
                    dynamic_cast<fesql::node::ColumnRefNode*>(exp_node);
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

}  // namespace vm
}  // namespace fesql
