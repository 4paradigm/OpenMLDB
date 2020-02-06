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
#include "analyser/analyser.h"
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
    explicit Planner(node::NodeManager *manager) : node_manager_(manager) {}
    virtual ~Planner() {}
    virtual int CreatePlanTree(
        const NodePointVector &parser_trees,
        PlanNodeList &plan_trees,  // NOLINT (runtime/references)
        Status &status) = 0;       // NOLINT (runtime/references)

 protected:
    void CreatePlanRecurse(const node::SQLNode *root, PlanNode *plan_tree,
                           Status &status);  // NOLINT (runtime/references)
    int CreateSelectPlan(const node::SQLNode *root, PlanNode *plan_tree,
                         Status &status);  // NOLINT (runtime/references)
    void CreateCreateTablePlan(const node::SQLNode *root,
                               node::CreatePlanNode *plan_tree,
                               Status &status);  // NOLINT (runtime/references)
    void CreateProjectPlanNode(const node::SQLNode *root, const uint32_t pos,
                               const std::string &table_name,
                               node::ProjectPlanNode *plan_tree,
                               Status &status);  // NOLINT (runtime/references)
    void CreateCmdPlan(const SQLNode *root, node::CmdPlanNode *plan_tree,
                       Status &status);  // NOLINT (runtime/references)
    void CreateInsertPlan(const SQLNode *root, node::InsertPlanNode *plan_tree,
                          Status &status);  // NOLINT (runtime/references)

    void CreateFuncDefPlan(const SQLNode *root,
                           node::FuncDefPlanNode *plan_tree,
                           Status &status);  // NOLINT (runtime/references)
    void CreateWindowPlanNode(
        node::WindowDefNode *w_ptr, node::WindowPlanNode * plan_node,
        Status &status);  // NOLINT (runtime/references)
    int64_t CreateFrameOffset(const node::FrameBound *bound,
                              Status &status);  // NOLINT (runtime/references)
    void CreateDataProviderPlanNode(const node::SQLNode *root,
                                    PlanNode *plan_tree,
                                    Status &status);  // NOLINT
                                                      // (runtime/references)
    void CreateDataCollectorPlanNode(
        const node::SQLNode *root, PlanNode *plan_tree,
        Status &status);  // NOLINT (runtime/references)
    node::NodeManager *node_manager_;
    void ReflectFunctionNode(const node::FnNodeList *src,
                             node::FnNodeList *dist, Status &status); // NOLINT (runtime/references))
    int CreateFnBlock(std::vector<node::FnNode *> vector, int start, int end,
                       int32_t  indent,
                       node::FnNodeList *block, Status &status);
};

class SimplePlanner : public Planner {
 public:
    explicit SimplePlanner(node::NodeManager *manager) : Planner(manager) {}
    int CreatePlanTree(const NodePointVector &parser_trees,
                       PlanNodeList &plan_trees,
                       Status &status);  // NOLINT (runtime/references)
};

// TODO(chenjing): move to executor module
void TransformTableDef(const std::string &table_name,
                       const NodePointVector &column_desc_list,
                       type::TableDef *table,
                       Status &status);  // NOLINT (runtime/references)
std::string GenerateName(const std::string prefix, int id);

}  // namespace plan
}  // namespace fesql

#endif  // SRC_PLAN_PLANNER_H_
