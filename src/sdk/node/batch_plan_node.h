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

#ifndef HYBRIDSE_INCLUDE_NODE_BATCH_PLAN_NODE_H_
#define HYBRIDSE_INCLUDE_NODE_BATCH_PLAN_NODE_H_

#include <string>
#include <vector>
#include "node/node_enum.h"
#include "node/sql_node.h"

namespace hybridse {
namespace node {

const std::string NameOfPlanNodeType(BatchPlanNodeType type);

class BatchPlanNode : public node::NodeBase<BatchPlanNode> {
 public:
    explicit BatchPlanNode(const BatchPlanNodeType& type)
        : type_(type), children_() {}
    virtual ~BatchPlanNode() {}

    const BatchPlanNodeType& GetType() const { return type_; }
    void AddChild(const BatchPlanNode* node) { children_.push_back(node); }
    const std::vector<const BatchPlanNode*>& GetChildren() const {
        return children_;
    }
    const std::vector<const BatchPlanNode*>& GetChildren() { return children_; }

    const std::string GetTypeName() const override {
        return NameOfPlanNodeType(type_);
    }

 private:
    BatchPlanNodeType type_;
    std::vector<const BatchPlanNode*> children_;
};

class DatasetNode : public BatchPlanNode {
 public:
    explicit DatasetNode(const std::string& table)
        : BatchPlanNode(kBatchDataset), table_(table) {}

    const std::string& GetTable() const { return table_; }

    bool Equals(const BatchPlanNode* other) const override;

 private:
    std::string table_;
};

// partition by multi column
class ColumnPartitionNode : public BatchPlanNode {
 public:
    explicit ColumnPartitionNode(const std::string& column)
        : BatchPlanNode(kBatchPartition), columns_() {
        columns_.push_back(column);
    }
    explicit ColumnPartitionNode(const std::vector<std::string>& columns)
        : BatchPlanNode(kBatchPartition), columns_(columns) {}

    ~ColumnPartitionNode() {}
    const std::vector<std::string>& GetColumns() const { return columns_; }

    bool Equals(const BatchPlanNode* other) const override;

 private:
    std::vector<std::string> columns_;
};

class MapNode : public BatchPlanNode {
 public:
    explicit MapNode(const NodePointVector& nodes)
        : BatchPlanNode(kBatchMap), nodes_(nodes) {}
    ~MapNode() {}
    const NodePointVector& GetNodes() const { return nodes_; }

    bool Equals(const BatchPlanNode* other) const override;

 private:
    const NodePointVector nodes_;
};

class BatchPlanTree {
 public:
    BatchPlanTree() : root_(NULL) {}
    ~BatchPlanTree() {}

    void SetRoot(BatchPlanNode* root) { root_ = root; }
    const BatchPlanNode* GetRoot() const { return root_; }

 private:
    BatchPlanNode* root_;
};

}  // namespace node
}  // namespace hybridse

#endif  // HYBRIDSE_INCLUDE_NODE_BATCH_PLAN_NODE_H_
