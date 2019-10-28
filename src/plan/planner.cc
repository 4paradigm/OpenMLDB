/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * planner.cc
 *      
 * Author: chenjing
 * Date: 2019/10/24 
 *--------------------------------------------------------------------------
**/
#include "planner.h"

namespace fesql {
namespace plan {

//Planner implementation
PlanNode *SimplePlanner::CreatePlan() {

    if (nullptr == parser_tree_ptr_) {
        LOG(WARNING) << "can not create plan with null parser tree";
        return nullptr;
    }

    return CreatePlanRecurse(parser_tree_ptr_);
}

PlanNode *Planner::CreatePlanRecurse(parser::SQLNode *root) {
    if (nullptr == root) {
        LOG(WARNING) << "return null plan node with null parser tree";
        return nullptr;
    }

    switch (root->GetType()) {
        case parser::kSelectStmt:return CreateSelectPlan((parser::SelectStmt *) root);
        default:return nullptr;

    }

}

/**
 * create simple select plan node:
 *  simple select:
 *      + from_list
 *          + from_node
 *              + table_ref_node
 *      + project_list
 *          + project_node
 *              + expression
 *                  +   op_expr
 *                      | function
 *                      | const
 *                      | column ref node
 *              + name
 *          + project_node
 *          + project_node
 *          + ..
 *      + limit_count
 *
 * @param root
 * @return select plan node
 */
PlanNode *Planner::CreateSelectPlan(parser::SelectStmt *root) {
    SelectPlanNode *select_plan = new SelectPlanNode();

    if (nullptr != root->GetTableRefList() && root->GetTableRefList()->Size() > 0) {
        parser::SQLNodeList *table_ref_list = root->GetTableRefList();


    }

    // set limit
    if (nullptr != root->GetLimit() && parser::kLimit == root->GetLimit()->GetType()) {
        parser::LimitNode *limit_ptr = (parser::LimitNode *) root->GetLimit();
        int count = limit_ptr->GetLimitCount();
        if (count <= 0) {
            LOG(WARNING) << "can not create select plan with limit <= 0";
        } else {
            select_plan->SetLimitCount(limit_ptr->GetLimitCount());
        }
    }

    // prepare project list plan node
    parser::SQLNodeList *select_expr_list = root->GetSelectList();
    if (nullptr != select_expr_list && 0 != select_expr_list->Size()) {

        ProjectListPlanNode *project_list_plan = new ProjectListPlanNode();
        parser::SQLLinkedNode *ptr = select_expr_list->GetHead();
        while (nullptr != ptr) {
            PlanNode *project_ndoe_ptr = CreateProjectPlanNode(ptr->node_ptr_);
            project_list_plan->AddChild(project_ndoe_ptr);
            ptr = ptr->next_;
        }
        select_plan->AddChild(project_list_plan);
    }

    return select_plan;
}

PlanNode *Planner::CreateProjectPlanNode(parser::SQLNode *root) {
    if (nullptr == root) {
        return nullptr;
    }

    switch (root->GetType()) {
        case parser::kResTarget: {
            parser::ResTarget *target_ptr = (parser::ResTarget *) root;
            return new ProjectPlanNode(target_ptr->GetVal(), target_ptr->GetName());
        }
        default: {
            LOG(ERROR) << "can not create project plan node with type " << parser::NameOfSQLNodeType(root->GetType());
        }

    }
}

PlanNode *Planner::CreateDataProviderPlanNode(parser::SQLNode *root) {
    return nullptr;
}

PlanNode *Planner::CreateDataCollectorPlanNode(parser::SQLNode *root) {
    return nullptr;
}

}
}
