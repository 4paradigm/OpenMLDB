/**
 * Copyright (c) 2023 OpenMLDB authors
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

#include "vm/internal/node_helper.h"

namespace hybridse {
namespace vm {
namespace internal {

Status GetDependentTables(const PhysicalOpNode* root, std::set<std::pair<std::string, std::string>>* db_tbs) {
    using OUT = std::set<std::pair<std::string, std::string>>;
    *db_tbs = internal::ReduceNode(
        root, OUT{},
        [](OUT init, const PhysicalOpNode* node) {
            if (node->GetOpType() == kPhysicalOpDataProvider) {
                auto* data_op = dynamic_cast<const PhysicalDataProviderNode*>(node);
                if (data_op != nullptr) {
                    init.emplace(data_op->GetDb(), data_op->GetName());
                }
            }
            return init;
        },
        [](const PhysicalOpNode* node) { return node->GetDependents(); });
    return Status::OK();
}

}  // namespace internal
}  // namespace vm
}  // namespace hybridse
