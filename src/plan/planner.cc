/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * planner.cc
 *      
 * Author: chenjing
 * Date: 2019/10/24 
 *--------------------------------------------------------------------------
**/
#include "plan/planner.h"

#include <map>

namespace fesql {
namespace plan {

//Planner implementation
PlanNode *SimplePlanner::CreatePlan(const SQLNode *parser_tree_ptr) {

    if (nullptr == parser_tree_ptr) {
        LOG(WARNING) << "can not create plan with null parser tree";
        return nullptr;
    }

    return CreatePlanRecurse(parser_tree_ptr);
}

PlanNode *Planner::CreatePlanRecurse(const SQLNode *root) {
    if (nullptr == root) {
        LOG(WARNING) << "return null plan node with null parser tree";
        return nullptr;
    }

    switch (root->GetType()) {
        case node::kSelectStmt:return CreateSelectPlan((const node::SelectStmt *) root);
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
PlanNode *Planner::CreateSelectPlan(const node::SelectStmt *root) {

    const node::NodePointVector& table_ref_list = root->GetTableRefList();

    if (table_ref_list.empty()) {
        LOG(ERROR) << "can not create select plan node with empty table references";
        return nullptr;
    }

    if (table_ref_list.size() > 1) {
        LOG(ERROR) << "can not create select plan node based on more than 2 tables";
        return nullptr;
    }

    const node::TableNode *table_node_ptr = (const node::TableNode *) table_ref_list.at(0);

    node::SelectPlanNode *select_plan = (node::SelectPlanNode *) node_manager_->MakePlanNode(node::kSelect);

    node::PlanNode * current_node = select_plan;

    std::map<std::string, node::ProjectListPlanNode *> project_list_map;
    // set limit
    if (nullptr != root->GetLimit()) {
        const node::LimitNode *limit_ptr = (node::LimitNode *) root->GetLimit();
        node::LimitPlanNode *limit_plan_ptr = (node::LimitPlanNode *) node_manager_->MakePlanNode(node::kPlanTypeLimit);
        limit_plan_ptr->SetLimitCnt(limit_ptr->GetLimitCount());
        current_node->AddChild(limit_plan_ptr);
        current_node = limit_plan_ptr;
    }


    // prepare project list plan node
    const node::NodePointVector& select_expr_list = root->GetSelectList();

    if (false == select_expr_list.empty()) {
        for (auto expr : select_expr_list) {
            node::ProjectPlanNode
                *project_node_ptr = CreateProjectPlanNode(expr, table_node_ptr->GetOrgTableName());
            if (nullptr == project_node_ptr) {
                LOG(WARNING) << "fail to create project plan node";
                continue;
            } else {
                std::string key =
                    project_node_ptr->GetW().empty() ? project_node_ptr->GetTable() : project_node_ptr->GetW();
                if (project_list_map.find(key) == project_list_map.end()) {
                    project_list_map[key] =
                        project_node_ptr->GetW().empty() ? node_manager_->MakeProjectListPlanNode(key, "") :
                        node_manager_->MakeProjectListPlanNode(project_node_ptr->GetTable(), key);
                }
                project_list_map[key]->AddProject(project_node_ptr);
            }
        }

        for (auto &v : project_list_map) {
            node::ProjectListPlanNode * project_list = v.second;
            project_list->AddChild(node_manager_->MakeSeqScanPlanNode(project_list->GetTable()));
            current_node->AddChild(v.second);
        }
    }

    return select_plan;
}

node::ProjectPlanNode *Planner::CreateProjectPlanNode(const SQLNode *root, const std::string& table_name) {
    if (nullptr == root) {
        return nullptr;
    }

    switch (root->GetType()) {
        case node::kResTarget: {
            const node::ResTarget *target_ptr = (const node::ResTarget *) root;
            std::string w = node::WindowOfExpression(target_ptr->GetVal());
            return node_manager_->MakeProjectPlanNode(target_ptr->GetVal(), target_ptr->GetName(), table_name, w);
        }
        default: {
            LOG(ERROR) << "can not create project plan node with type " << node::NameOfSQLNodeType(root->GetType());
            return nullptr;
        }
    }

}

PlanNode *Planner::CreateDataProviderPlanNode(const SQLNode *root) {
    return nullptr;
}

PlanNode *Planner::CreateDataCollectorPlanNode(const SQLNode *root) {
    return nullptr;
}

}
}
