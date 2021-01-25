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

#include <map>
#include <string>
#include <vector>
#include "base/fe_status.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "parser/parser.h"
#include "proto/fe_type.pb.h"
namespace fesql {
namespace plan {

using base::Status;
using node::NodePointVector;
using node::PlanNode;
using node::PlanNodeList;
using node::SQLNode;

class Planner {
 public:
    Planner(node::NodeManager *manager, const bool is_batch_mode,
            const bool is_cluster_optimized)
        : is_batch_mode_(is_batch_mode),
          is_cluster_optimized_(is_cluster_optimized),
          enable_window_maxsize_merged_(true),
          node_manager_(manager) {}
    virtual ~Planner() {}
    virtual int CreatePlanTree(
        const NodePointVector &parser_trees,
        PlanNodeList &plan_trees,  // NOLINT (runtime/references)
        Status &status) = 0;       // NOLINT (runtime/references)
    bool MergeWindows(const std::map<const node::WindowDefNode *,
                                     node::ProjectListNode *> &map,
                      std::vector<const node::WindowDefNode *> *windows);
    bool ExpandCurrentHistoryWindow(
        std::vector<const node::WindowDefNode *> *windows);
    const bool is_batch_mode_;
    const bool is_cluster_optimized_;
    const bool enable_window_maxsize_merged_;

 protected:
    bool IsTable(node::PlanNode *node);
    bool ValidatePrimaryPath(
        node::PlanNode *node, node::PlanNode **output,
        base::Status &status);  // NOLINT (runtime/references)
    bool CheckWindowFrame(const node::WindowDefNode *w_ptr,
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
    bool CreateWindowPlanNode(const node::WindowDefNode *w_ptr,
                              node::WindowPlanNode *plan_node,
                              Status &status);  // NOLINT (runtime/references)
    bool CreateCreateProcedurePlan(
        const node::SQLNode *root, const PlanNodeList &inner_plan_node_list,
        node::PlanNode **output,
        Status &status);  // NOLINT (runtime/references)
    node::NodeManager *node_manager_;
    std::string MakeTableName(const PlanNode *node) const;
    bool MergeProjectMap(
        const std::map<const node::WindowDefNode *, node::ProjectListNode *>
            &map,
        std::map<const node::WindowDefNode *, node::ProjectListNode *> *output,
        Status &status);  // NOLINT (runtime/references)
};

class SimplePlanner : public Planner {
 public:
    explicit SimplePlanner(node::NodeManager *manager)
        : Planner(manager, true, false) {}
    SimplePlanner(node::NodeManager *manager, bool is_batch_mode,
                  bool is_cluster_optimized = false)
        : Planner(manager, is_batch_mode, is_cluster_optimized) {}
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
