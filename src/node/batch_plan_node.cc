/*
 * Copyright (C) 4Paradigm
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
#include "node/batch_plan_node.h"

#include <string>
#include <vector>
#include "node/node_enum.h"
#include "node/sql_node.h"

namespace fesql {
namespace node {

bool DatasetNode::Equals(const BatchPlanNode *other) const {
    auto dataset = dynamic_cast<const DatasetNode *>(other);
    return dataset != nullptr && table_ == dataset->table_;
}

bool ColumnPartitionNode::Equals(const BatchPlanNode *other) const {
    auto column_partition = dynamic_cast<const ColumnPartitionNode *>(other);
    if (column_partition == nullptr) {
        return false;
    }
    if (columns_.size() != column_partition->columns_.size()) {
        return false;
    }
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i] != column_partition->columns_[i]) {
            return false;
        }
    }
    return true;
}

bool MapNode::Equals(const BatchPlanNode *other) const {
    auto map = dynamic_cast<const MapNode *>(other);
    if (map == nullptr) {
        return false;
    }
    if (nodes_.size() != map->nodes_.size()) {
        return false;
    }
    for (size_t i = 0; i < nodes_.size(); ++i) {
        if (nodes_[i] == nullptr || map->nodes_[i] == nullptr) {
            if (nodes_[i] != map->nodes_[i]) {
                return false;
            }
        } else {
            if (nodes_[i]->Equals(map->nodes_[i])) {
                return false;
            }
        }
    }
    return true;
}

const std::string NameOfPlanNodeType(BatchPlanNodeType type) {
    switch (type) {
        case kBatchDataset:
            return "kBatchDataset";
        case kBatchPartition:
            return "kBatchPartition";
        case kBatchMap:
            return "kBatchMap";
        default:
            return "unknown";
    }
}

}  // namespace node
}  // namespace fesql
