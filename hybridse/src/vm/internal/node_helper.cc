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
absl::StatusOr<PhysicalOpNode*> ExtractRequestNode(PhysicalOpNode* in) {
    if (in == nullptr) {
        return absl::InvalidArgumentError("null input node");
    }

    switch (in->GetOpType()) {
        case vm::kPhysicalOpDataProvider: {
            auto tp = dynamic_cast<PhysicalDataProviderNode*>(in)->provider_type_;
            if (tp == kProviderTypeRequest) {
                return in;
            }

            // else data provider is fine inside node tree,
            // generally it is of type Partition, but can be Table as well e.g window (t1 instance_not_in_window)
            return nullptr;
        }
        case vm::kPhysicalOpSetOperation: {
            return ExtractRequestNode(in->GetProducer(0));
        }
        case vm::kPhysicalOpJoin:
        case vm::kPhysicalOpPostRequestUnion:
        case vm::kPhysicalOpRequestUnion:
        case vm::kPhysicalOpRequestAggUnion:
        case vm::kPhysicalOpRequestJoin: {
            // Binary Node
            // - left or right status not ok -> error
            // - left and right both has non-null value
            //   - the two not equals -> error
            // - otherwise -> left as request node
            auto left = ExtractRequestNode(in->GetProducer(0));
            if (!left.ok()) {
                return left;
            }
            auto right = ExtractRequestNode(in->GetProducer(1));
            if (!right.ok()) {
                return right;
            }

            if (left.value() != nullptr && right.value() != nullptr) {
                if (!left.value()->Equals(right.value())) {
                    return absl::NotFoundError(
                        absl::StrCat("different request table from left and right path:\n", in->GetTreeString()));
                }
            }

            return left.value();
        }
        default: {
            break;
        }
    }

    if (in->GetProducerCnt() == 0) {
        // leaf node excepting DataProdiverNode
        // consider ok as right source from one of the supported binary op
        return nullptr;
    }

    if (in->GetProducerCnt() > 1) {
        return absl::UnimplementedError(
            absl::StrCat("Non-support op with more than one producer:\n", in->GetTreeString()));
    }

    return ExtractRequestNode(in->GetProducer(0));
}
}  // namespace internal
}  // namespace vm
}  // namespace hybridse
