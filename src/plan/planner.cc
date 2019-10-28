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

    parser::NodePointVector table_ref_list = root->GetTableRefList();

    if (table_ref_list.empty()) {
        LOG(ERROR) << "can not create select plan node with empty table references";
        return nullptr;
    }

    if (table_ref_list.size() > 1) {
        LOG(ERROR) << "can not create select plan node based on more than 2 tables";
        return nullptr;
    }

    parser::TableNode *table_node_ptr = (parser::TableNode *) table_ref_list.at(0);
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
    parser::NodePointVector select_expr_list = root->GetSelectList();

    if (false == select_expr_list.empty()) {
        for (auto expr : select_expr_list) {
            ProjectPlanNode
                *project_node_ptr = (ProjectPlanNode *) CreateProjectPlanNode(expr, table_node_ptr->GetOrgTableName());
            if (nullptr == project_node_ptr) {
                LOG(WARNING) << "fail to create project plan node";
                continue;
            } else {
                std::string key =
                    project_node_ptr->GetW().empty() ? project_node_ptr->GetTable() : project_node_ptr->GetW();
                if (project_list_map.find(key) == project_list_map.end()) {
                    project_list_map[key] = project_node_ptr->GetW().empty() ? new ProjectListPlanNode(key, "") :
                                            new ProjectListPlanNode(project_node_ptr->GetTable(), key);
                }
                project_list_map[key]->AddProject(project_node_ptr);
            }
        }

        for (auto &v : project_list_map) {
            select_plan->AddChild(v.second);
        }
    }

    return select_plan;
}

PlanNode *Planner::CreateProjectPlanNode(parser::SQLNode *root, std::string table_name) {
    if (nullptr == root) {
        return nullptr;
    }

    switch (root->GetType()) {
        case parser::kResTarget: {
            parser::ResTarget *target_ptr = (parser::ResTarget *) root;

            std::string w = parser::WindowOfExpression(target_ptr->GetVal());
            return new ProjectPlanNode(target_ptr->GetVal(), target_ptr->GetName(), table_name, w);
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
