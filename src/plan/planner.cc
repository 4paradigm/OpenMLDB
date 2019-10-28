/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * planner.cc
 *      
 * Author: chenjing
 * Date: 2019/10/24 
 *--------------------------------------------------------------------------
**/
#include "planner.h"
#include <map>"
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

    parser::SQLNodeList *table_ref_list = root->GetTableRefList();

    if (nullptr == table_ref_list || 0 == table_ref_list->Size()) {
        LOG(ERROR) << "can not create select plan node with empty table references";
        return nullptr;
    }

    if (table_ref_list->Size() > 1) {
        LOG(ERROR) << "can not create select plan node based on more than 2 tables";
        return nullptr;
    }

    if (nullptr == table_ref_list->GetHead() || nullptr == table_ref_list->GetHead()->node_ptr_
        || parser::kTable != table_ref_list->GetHead()->node_ptr_->GetType()) {
        LOG(ERROR) << "can not create select plan node based on null or empty table reference";
    }

    parser::TableNode *table_node_ptr = (parser::TableNode *) (table_ref_list->GetHead()->node_ptr_);
    SelectPlanNode *select_plan = new SelectPlanNode();
    std::map<std::string, ProjectListPlanNode *> project_list_map;
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

        parser::SQLLinkedNode *ptr = select_expr_list->GetHead();
        while (nullptr != ptr) {
            ProjectPlanNode *project_node_ptr = (ProjectPlanNode *) CreateProjectPlanNode(ptr->node_ptr_);
            if (nullptr == project_node_ptr) {
                LOG(WARNING) << "fail to create project plan node";
                ptr = ptr->next_;
                continue;
            } else {
                std::string key =
                    project_node_ptr->GetW().empty() ? table_node_ptr->GetOrgTableName() : project_node_ptr->GetW();
                if (project_list_map.find(key) == project_list_map.end()) {
                    project_list_map[key] = project_node_ptr->GetW().empty() ? new ProjectListPlanNode() :
                                            new ProjectListPlanNode(key);
                }
                project_list_map[key]->AddProject(project_node_ptr);
                ptr = ptr->next_;
            }
        }

        for (auto &v : project_list_map) {
            select_plan->AddChild(v.second);
        }
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

            std::string w = parser::WindowOfExpression(target_ptr->GetVal());
            return new ProjectPlanNode(target_ptr->GetVal(), target_ptr->GetName(), w);
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
