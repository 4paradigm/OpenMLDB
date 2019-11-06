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
int Planner::CreateSelectPlan(node::SQLNode *select_tree,
                              PlanNode *plan_tree) {
    node::SelectStmt *root = (node::SelectStmt *) select_tree;
    node::SelectPlanNode *select_plan = (node::SelectPlanNode *) plan_tree;

    node::NodePointVector table_ref_list = root->GetTableRefList();

    if (table_ref_list.empty()) {
        LOG(ERROR)
            << "can not create select plan node with empty table references";
        return error::kPlanErrorTableRefIsEmpty;
    }

    if (table_ref_list.size() > 1) {
        LOG(ERROR)
            << "can not create select plan node based on more than 2 tables";
        return error::kPlanErrorQueryMultiTable;
    }

    node::TableNode *table_node_ptr = (node::TableNode *) table_ref_list.at(0);

    node::PlanNode *current_node = select_plan;

    std::map<std::string, node::ProjectListPlanNode *> project_list_map;
    // set limit
    if (nullptr != root->GetLimit()) {
        node::LimitNode *limit_ptr = (node::LimitNode *) root->GetLimit();
        node::LimitPlanNode *limit_plan_ptr =
            (node::LimitPlanNode *) node_manager_->MakePlanNode(
                node::kPlanTypeLimit);
        limit_plan_ptr->SetLimitCnt(limit_ptr->GetLimitCount());
        current_node->AddChild(limit_plan_ptr);
        current_node = limit_plan_ptr;
    }



    // prepare project list plan node
    node::NodePointVector select_expr_list = root->GetSelectList();

    if (false == select_expr_list.empty()) {
        for (auto expr : select_expr_list) {
            node::ProjectPlanNode *project_node_ptr =
                (node::ProjectPlanNode *)
                    (node_manager_->MakePlanNode(node::kProject));

            int ret = CreateProjectPlanNode(expr,
                                            table_node_ptr->GetOrgTableName(),
                                            project_node_ptr);
            if (0 != ret) {
                LOG(WARNING) << "fail to create project plan node";
                return ret;
            }

            std::string key =
                project_node_ptr->GetW().empty() ? project_node_ptr->GetTable()
                                                 : project_node_ptr->GetW();
            if (project_list_map.find(key) == project_list_map.end()) {
                project_list_map[key] =
                    project_node_ptr->GetW().empty()
                    ? node_manager_->MakeProjectListPlanNode(key, "") :
                    node_manager_->MakeProjectListPlanNode(
                        project_node_ptr->GetTable(), key);
            }
            project_list_map[key]->AddProject(project_node_ptr);
        }

        for (auto &v : project_list_map) {
            node::ProjectListPlanNode *project_list = v.second;
            project_list->AddChild(node_manager_->MakeSeqScanPlanNode(
                project_list->GetTable()));
            current_node->AddChild(v.second);
        }
    }

    return 0;
}

int Planner::CreateProjectPlanNode(SQLNode *root,
                                   std::string table_name,
                                   node::ProjectPlanNode *plan_tree) {
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
            LOG(ERROR) << "can not create project plan node with type "
                       << node::NameOfSQLNodeType(root->GetType());
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
int Planner::CreateCreateTablePlan(node::SQLNode *root,
                                   node::CreatePlanNode *plan_tree) {
    node::CreateStmt *create_tree = (node::CreateStmt *) root;
    plan_tree->SetColumnDescList(create_tree->GetColumnDefList());
    plan_tree->setTableName(create_tree->GetTableName());
    return 0;
}

int SimplePlanner::CreatePlanTree(NodePointVector &parser_trees,
                                  PlanNodeList &plan_trees) {
    if (parser_trees.empty()) {
        LOG(WARNING) << "handle empty parser trees";
        return error::kSucess;
    }

    for (auto parser_tree : parser_trees) {
        switch (parser_tree->GetType()) {
            case node::kSelectStmt: {
                PlanNode *select_plan =
                    node_manager_->MakePlanNode(node::kPlanTypeSelect);
                int ret = CreateSelectPlan(parser_tree, select_plan);
                if (0 != ret) {
                    return ret;
                }
                plan_trees.push_back(select_plan);
                break;
            }
            case node::kCreateStmt: {
                PlanNode *create_plan =
                    node_manager_->MakePlanNode(node::kPlanTypeCreate);
                int ret = CreateCreateTablePlan(
                    parser_tree, (node::CreatePlanNode *) create_plan);
                if (0 != ret) {
                    return ret;
                }
                plan_trees.push_back(create_plan);
                break;
            }
            default: {
                LOG(WARNING) << "can not handle tree type "
                             << node::NameOfSQLNodeType(
                                 parser_tree->GetType());
                return error::kPlanErrorUnSupport;
            }
        }
    }
    return error::kSucess;
}

int transformTableDef(std::string table_name,
                               NodePointVector &column_desc_list,
                               type::TableDef &table) {

    std::set<std::string> index_names;
    std::set<std::string> column_names;

    for (auto column_desc : column_desc_list) {
        switch (column_desc->GetType()) {
            case node::kColumnDesc: {
                node::ColumnDefNode *column_def = (node::ColumnDefNode *)
                    column_desc;
                type::ColumnDef *column = table.add_columns();

                if (column_names.find(column_def->GetColumnName()) !=
                    column_names.end()) {
                    LOG(WARNING) << "CREATE error: COLUMN NAME "
                                 << column_def->GetColumnName()
                                 << " duplicate";
                    return error::kCreateErrorDuplicationColumnName;
                }
                column->set_name(column_def->GetColumnName());
                column_names.insert(column_def->GetColumnName());
                switch (column_def->GetColumnType()) {
                    case node::kTypeBool:column->set_type(type::Type::kBool);
                        break;
                    case node::kTypeInt32:column->set_type(type::Type::kInt32);
                        break;
                    case node::kTypeInt64:column->set_type(type::Type::kInt64);
                        break;
                    case node::kTypeFloat:column->set_type(type::Type::kFloat);
                        break;
                    case node::kTypeDouble:column->set_type(type::Type::kDouble);
                        break;
                    case node::kTypeTimestamp: {
                        column->set_type(type::Type::kTimestamp);
                        break;
                    }
                    case node::kTypeString:column->set_type(type::Type::kString);
                        break;
                    default: {
                        LOG(WARNING)
                            << "CREATE error: column type "
                            << node::DataTypeName(column_def->GetColumnType())
                            << " is not supported";
                        return error::kCreateErrorUnSupportColumnType;
                    }
                }
                break;
            }

            case node::kColumnIndex: {
                node::ColumnIndexNode *column_index = (node::ColumnIndexNode *)
                    column_desc;

                if (column_index->GetName().empty()) {
                    column_index->SetName(
                        GenerateName("INDEX", table.indexes_size()));
                }
                if (index_names.find(column_index->GetName()) !=
                    index_names.end()) {
                    LOG(WARNING) << "CREATE error: INDEX NAME "
                                 << column_index->GetName()
                                 << " duplicate";
                    return error::kCreateErrorDuplicationIndexName;
                }
                index_names.insert(column_index->GetName());
                type::IndexDef *index = table.add_indexes();
                index->set_name(column_index->GetName());

                // TODO(chenjing): set ttl per key
                if (-1 != column_index->GetTTL()) {
                    index->add_ttl(column_index->GetTTL());
                }

                for (auto key : column_index->GetKey()) {
                    index->add_first_keys(key);
                }

                if (!column_index->GetTs().empty()) {
                    index->set_second_key(column_index->GetTs());
                }
                break;
            }
            default: {
                LOG(WARNING) << "can not support " <<
                             node::NameOfSQLNodeType
                                 (column_desc->GetType())
                             << " when CREATE TABLE";
                return error::kAnalyserErrorUnSupport;
            }
        }

    }
    table.set_name(table_name);
    return 0;
}

std::string GenerateName(const std::string prefix, int id) {
    time_t t;
    time(&t);
    std::string name = prefix + "_" + std::to_string(id) + "_" +
        std::to_string(t);
    return name;
}
} // namespace  plan
} // namespace fesql
