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
#include <vector>
#include "base/status.h"
#include "glog/logging.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "parser/parser.h"
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
    explicit Planner(node::NodeManager *manager)
        : mode_(node::kPlanModeBatch), node_manager_(manager) {}
    Planner(node::NodeManager *manager, node::PlanModeType mode)
        : mode_(mode), node_manager_(manager) {}
    virtual ~Planner() {}
    virtual int CreatePlanTree(
        const NodePointVector &parser_trees,
        PlanNodeList &plan_trees,  // NOLINT (runtime/references)
        Status &status) = 0;       // NOLINT (runtime/references)
    const node::PlanModeType mode_;

 protected:
    bool ValidatePrimaryPath(
        node::PlanNode *node, node::PlanNode **output,
        base::Status &status);  // NOLINT (runtime/references)
    void CreatePlanRecurse(const node::SQLNode *root, PlanNode *plan_tree,
                           Status &status);  // NOLINT (runtime/references)
    bool CreateQueryPlan(const node::QueryNode *root, PlanNode **plan_tree,
                         Status &status);  // NOLINT (runtime/references)
    bool CreateSelectQueryPlan(const node::SelectQueryNode *root,
                               PlanNode **plan_tree,
                               Status &status);  // NOLINT (runtime/references)
    bool CreateUnionQueryPlan(const node::UnionQueryNode *root,
                              PlanNode **plan_tree,
                              Status &status);  // NOLINT (runtime/references)
    bool CreateCreateTablePlan(const node::SQLNode *root,
                               node::PlanNode **output,
                               Status &status);  // NOLINT (runtime/references)
    bool CreateTableReferencePlanNode(
        const node::TableRefNode *root, node::PlanNode **output,
        Status &status);  // NOLINT (runtime/references)
    bool CreateCmdPlan(const SQLNode *root, node::PlanNode **output,
                       Status &status);  // NOLINT (runtime/references)
    bool CreateInsertPlan(const SQLNode *root, node::PlanNode **output,
                          Status &status);  // NOLINT (runtime/references)

    bool CreateFuncDefPlan(const SQLNode *root, node::PlanNode **output,
                           Status &status);  // NOLINT (runtime/references)
    bool CreateWindowPlanNode(node::WindowDefNode *w_ptr,
                              node::WindowPlanNode *plan_node,
                              Status &status);  // NOLINT (runtime/references)
    int64_t CreateFrameOffset(const node::FrameBound *bound,
                              Status &status);  // NOLINT (runtime/references)
    node::NodeManager *node_manager_;
    std::string MakeTableName(const PlanNode *node) const;
    bool MergeProjectList(node::ProjectListNode *project_list1,
                          node::ProjectListNode *project_list2,
                          node::ProjectListNode *merged_project);
};

class SimplePlanner : public Planner {
 public:
    explicit SimplePlanner(node::NodeManager *manager) : Planner(manager) {}
    SimplePlanner(node::NodeManager *manager, node::PlanModeType mode)
        : Planner(manager, mode) {}
    int CreatePlanTree(const NodePointVector &parser_trees,
                       PlanNodeList &plan_trees,
                       Status &status);  // NOLINT (runtime/references)
};

// TODO(chenjing): move to executor module
bool TransformTableDef(const std::string &table_name,
                       const NodePointVector &column_desc_list,
                       type::TableDef *table,
                       Status &status);  // NOLINT (runtime/references)
std::string GenerateName(const std::string prefix, int id);

}  // namespace plan
}  // namespace fesql

#endif  // SRC_PLAN_PLANNER_H_
