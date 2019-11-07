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

using node::NodePointVector;
using node::PlanNode;
using node::PlanNodeList;
using node::SQLNode;
class Planner {
 public:
  explicit Planner(node::NodeManager *manager) : node_manager_(manager) {}

  virtual ~Planner() {}

  virtual int CreatePlanTree(const NodePointVector &parser_trees,
                             PlanNodeList &plan_trees) = 0;

 protected:
  int CreatePlanRecurse(node::SQLNode *root, PlanNode *plan_tree);
  int CreateSelectPlan(node::SQLNode *root, PlanNode *plan_tree);
  int CreateCreateTablePlan(node::SQLNode *root,
                            node::CreatePlanNode *plan_tree);
  int CreateProjectPlanNode(node::SQLNode *root, std::string table_name,
                            node::ProjectPlanNode *plan_tree);
  int CreateDataProviderPlanNode(node::SQLNode *root, PlanNode *plan_tree);
  int CreateDataCollectorPlanNode(node::SQLNode *root, PlanNode *plan_tree);

  node::NodeManager *node_manager_;
};

class SimplePlanner : public Planner {
 public:
  explicit SimplePlanner(node::NodeManager *manager) : Planner(manager) {}
  int CreatePlanTree(const NodePointVector &parser_trees,
                     PlanNodeList &plan_trees);
};

// TODO(chenjing): move to executor module
int transformTableDef(const std::string &table_name,
                      const NodePointVector &column_desc_list,
                      type::TableDef *table);
std::string GenerateName(const std::string prefix, int id);

}  // namespace plan
}  // namespace fesql

#endif  // SRC_PLAN_PLANNER_H_
