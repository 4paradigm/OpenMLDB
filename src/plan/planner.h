/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * plan.h
 *
 * Author: chenjing
 * Date: 2019/10/24
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_PLAN_PLANNER_H_
#define SRC_PLAN_PLANNER_H_

#include <string>
#include "glog/logging.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "proto/type.pb.h"
namespace fesql {
namespace plan {

using base::Status;
using node::NodePointVector;
using node::PlanNode;
using node::PlanNodeList;
using node::SQLNode;
class Planner {
 public:
    explicit Planner(node::NodeManager *manager) : node_manager_(manager) {}
    virtual ~Planner() {}
    virtual int CreatePlanTree(
        const NodePointVector &parser_trees,
        PlanNodeList &plan_trees,  // NOLINT (runtime/references)
        Status &status) = 0;       // NOLINT (runtime/references)

 protected:
    void CreatePlanRecurse(node::SQLNode *root, PlanNode *plan_tree,
                           Status &status);  // NOLINT (runtime/references)
    int CreateSelectPlan(node::SQLNode *root, PlanNode *plan_tree,
                         Status &status);  // NOLINT (runtime/references)
    void CreateCreateTablePlan(node::SQLNode *root,
                               node::CreatePlanNode *plan_tree,
                               Status &status);  // NOLINT (runtime/references)
    void CreateProjectPlanNode(node::SQLNode *root, std::string table_name,
                               node::ProjectPlanNode *plan_tree,
                               Status &status);  // NOLINT (runtime/references)
    void CreateDataProviderPlanNode(
        node::SQLNode *root, PlanNode *plan_tree,
        Status &status);  // NOLINT (runtime/references)
    void CreateDataCollectorPlanNode(
        node::SQLNode *root, PlanNode *plan_tree,
        Status &status);  // NOLINT (runtime/references)

    node::NodeManager *node_manager_;
};

class SimplePlanner : public Planner {
 public:
    explicit SimplePlanner(node::NodeManager *manager) : Planner(manager) {}
    int CreatePlanTree(const NodePointVector &parser_trees,
                       PlanNodeList &plan_trees,
                       Status &status);  // NOLINT (runtime/references)
};

// TODO(chenjing): move to executor module
void transformTableDef(const std::string &table_name,
                       const NodePointVector &column_desc_list,
                       type::TableDef *table,
                       Status &status);  // NOLINT (runtime/references)
std::string GenerateName(const std::string prefix, int id);

}  // namespace plan
}  // namespace fesql

#endif  // SRC_PLAN_PLANNER_H_
