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
int Planner::CreateSelectPlan(node::SQLNode *select_tree, PlanNode *plan_tree) {

    node::SelectStmt *root = (node::SelectStmt *) select_tree;
    node::SelectPlanNode *select_plan = (node::SelectPlanNode *) plan_tree;

    node::NodePointVector table_ref_list = root->GetTableRefList();

    if (table_ref_list.empty()) {
        LOG(ERROR) << "can not create select plan node with empty table references";
        return error::kPlanErrorTableRefIsEmpty;
    }

    if (table_ref_list.size() > 1) {
        LOG(ERROR) << "can not create select plan node based on more than 2 tables";
        return error::kPlanErrorQueryMultiTable;
    }

    node::TableNode *table_node_ptr = (node::TableNode *) table_ref_list.at(0);

    node::PlanNode *current_node = select_plan;

    std::map<std::string, node::ProjectListPlanNode *> project_list_map;
    // set limit
    if (nullptr != root->GetLimit()) {
        node::LimitNode *limit_ptr = (node::LimitNode *) root->GetLimit();
        node::LimitPlanNode *limit_plan_ptr = (node::LimitPlanNode *) node_manager_->MakePlanNode(node::kPlanTypeLimit);
        limit_plan_ptr->SetLimitCnt(limit_ptr->GetLimitCount());
        current_node->AddChild(limit_plan_ptr);
        current_node = limit_plan_ptr;
    }



    // prepare project list plan node
    node::NodePointVector select_expr_list = root->GetSelectList();

    if (false == select_expr_list.empty()) {
        for (auto expr : select_expr_list) {
            node:
            node::ProjectPlanNode
                *project_node_ptr = (node::ProjectPlanNode *) (node_manager_->MakePlanNode(node::kProject));

            int ret = CreateProjectPlanNode(expr, table_node_ptr->GetOrgTableName(), project_node_ptr);
            if (0 != ret) {
                LOG(WARNING) << "fail to create project plan node";
                return ret;
            }

            std::string key =
                project_node_ptr->GetW().empty() ? project_node_ptr->GetTable() : project_node_ptr->GetW();
            if (project_list_map.find(key) == project_list_map.end()) {
                project_list_map[key] =
                    project_node_ptr->GetW().empty() ? node_manager_->MakeProjectListPlanNode(key, "") :
                    node_manager_->MakeProjectListPlanNode(project_node_ptr->GetTable(), key);
            }
            project_list_map[key]->AddProject(project_node_ptr);
        }

        for (auto &v : project_list_map) {
            node::ProjectListPlanNode *project_list = v.second;
            project_list->AddChild(node_manager_->MakeSeqScanPlanNode(project_list->GetTable()));
            current_node->AddChild(v.second);
        }
    }

    return 0;
}

int Planner::CreateProjectPlanNode(SQLNode *root, std::string table_name, node::ProjectPlanNode *plan_tree) {
    if (nullptr == root) {
        return error::kPlanErrorNullNode;
    }

    switch (root->GetType()) {
        case node::kResTarget: {
            node::ResTarget *target_ptr = (node::ResTarget *) root;
            std::string w = node::WindowOfExpression(target_ptr->GetVal());
            plan_tree->SetW(w);
            plan_tree->SetName(target_ptr->GetName());
            plan_tree->SetExpression(target_ptr->GetVal());
            plan_tree->SetTable(table_name);
            return 0;
        }
        default: {
            LOG(ERROR) << "can not create project plan node with type " << node::NameOfSQLNodeType(root->GetType());
            return error::kPlanErrorUnSupport;
        }

    }

    return 0;
}

int Planner::CreateDataProviderPlanNode(SQLNode *root, PlanNode *plan_tree) {
    return 0;
}

int Planner::CreateDataCollectorPlanNode(SQLNode *root, PlanNode *plan_tree) {
    return 0;
}

int SimplePlanner::CreatePlanTree(NodePointVector &parser_trees, PlanNodeList &plan_trees) {
    if (parser_trees.empty()) {
        LOG(WARNING) << "handle empty parser trees";
        return error::kSucess;
    }

    for (auto parser_tree : parser_trees) {
        switch (parser_tree->GetType()) {
            case node::kSelectStmt: {
                PlanNode *select_plan = node_manager_->MakePlanNode(node::kPlanTypeSelect);
                int ret = CreateSelectPlan(parser_tree, select_plan);
                if (0 != ret) {
                    return ret;
                }
                plan_trees.push_back(select_plan);
                break;
            }
            default: {
                LOG(WARNING) << "can not handle tree type " << node::NameOfSQLNodeType(parser_tree->GetType());
                return error::kPlanErrorUnSupport;
            }
        }
    }
    return error::kSucess;
}
}
}
