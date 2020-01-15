/*
 * batch_plan_node.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#ifndef SRC_NODE_BATCH_PLAN_NODE_H_
#define SRC_NODE_BATCH_PLAN_NODE_H_

#include "node/sql_node.h"
#include "node/node_enum.h"

namespace fesql {
namespace node {

class BatchPlanNode {
 public:
    explicit BatchPlanNode(const BatchPlanNodeType& type):type_(type), children_() {}
    virtual ~BatchPlanNode() {}

    const BatchPlanNodeType& GetType() const { return type_;}
    void AddChild(const BatchPlanNode* node) {
        children_.push_back(node);
    }
 private:
    BatchPlanNodeType type_;
    std::vector<const BatchPlanNode*> children_;
};

class DatasetNode : public BatchPlanNode {
 public:
    explicit DatasetNode(const std::string& table):BatchPlanNode(kBatchDataset), table_(table) {}

    const std::string& GetTable() const {
        return table_;
    }
 private:
    std::string table_;
};

// partition by multi column
class ColumnPartitionNode : public BatchPlanNode {
 public:
    explicit ColumnPartitionNode(const std::string& column):BatchPlanNode(kBatchPartition), 
    columns_(){
        columns_.push_back(column);
    }
    ColumnPartitionNode(const std::vector<std::string>& columns):BatchPlanNode(kBatchPartition),
    columns_(columns) {}

    ~ColumnPartitionNode() {}
    const std::vector<std::string>& GetColumns() const {
        return columns_;
    }
 private:
    std::vector<std::string> columns_;
};

class MapNode : public BatchPlanNode {
 public:
    explicit MapNode(const NodePointVector& nodes ):BatchPlanNode(kBatchMap), nodes_(nodes) {}
    ~MapNode() {}
    const NodePointVector& GetNodes() const {
        return nodes_;
    }
 private:
    const NodePointVector nodes_;
};

class BatchPlanTree {
 public:
    BatchPlanTree():root_(NULL){}
    ~BatchPlanTree() {}

    void SetRoot(BatchPlanNode* root) {
        root_ = root;
    }
    const BatchPlanNode* GetRoot() const {
        return root_;
    }
 private:
    BatchPlanNode* root_;
};

}  // namespace node
}  // namespace fesql

#endif  // SRC_NODE_BATCH_PLAN_NODE_H 
