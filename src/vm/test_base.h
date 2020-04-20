/*
 * test_base.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_VM_TEST_BASE_H_
#define SRC_VM_TEST_BASE_H_

#include <memory>
#include <sstream>
#include "glog/logging.h"
#include "tablet/tablet_catalog.h"
#include "vm/catalog.h"

namespace fesql {
namespace vm {

void ExtractExprFromSimpleSQL(::fesql::node::NodeManager* nm,
                            std::string& sql, node::ExprNode** output) {
    boost::to_lower(sql);
    std::cout << sql << std::endl;
    ::fesql::node::PlanNodeList plan_trees;
    ::fesql::base::Status base_status;
    ::fesql::plan::SimplePlanner planner(nm);
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodePointVector parser_trees;
    parser.parse(sql, parser_trees, nm, base_status);
    ASSERT_EQ(0, base_status.code);
    if (planner.CreatePlanTree(parser_trees, plan_trees, base_status) == 0) {
        std::cout << base_status.msg;
        std::cout << *(plan_trees[0]) << std::endl;
    } else {
        std::cout << base_status.msg;
    }
    ASSERT_EQ(0, base_status.code);
    std::cout.flush();

    ASSERT_EQ(node::kPlanTypeProject, plan_trees[0]->GetChildren()[0]->type_);
    auto project_plan_node =
        dynamic_cast<node::ProjectPlanNode*>(plan_trees[0]->GetChildren()[0]);
    ASSERT_EQ(1u, project_plan_node->project_list_vec_.size());

    auto project_list = dynamic_cast<node::ProjectListNode*>(project_plan_node->project_list_vec_[0]);
    auto project = dynamic_cast<node::ProjectNode*>(project_list->GetProjects()[0]);
    *output = project->GetExpression();
}

bool AddTable(const std::shared_ptr<tablet::TabletCatalog>& catalog,
              const fesql::type::TableDef& table_def,
              std::shared_ptr<fesql::storage::Table> table) {
    std::shared_ptr<tablet::TabletTableHandler> handler(
        new tablet::TabletTableHandler(table_def.columns(), table_def.name(),
                                       table_def.catalog(), table_def.indexes(),
                                       table));
    bool ok = handler->Init();
    if (!ok) {
        return false;
    }
    return catalog->AddTable(handler);
}
std::shared_ptr<tablet::TabletCatalog> BuildCommonCatalog(
    const fesql::type::TableDef& table_def,
    std::shared_ptr<fesql::storage::Table> table) {
    std::shared_ptr<tablet::TabletCatalog> catalog(new tablet::TabletCatalog());
    bool ok = catalog->Init();
    if (!ok) {
        return std::shared_ptr<tablet::TabletCatalog>();
    }
    if (!AddTable(catalog, table_def, table)) {
        return std::shared_ptr<tablet::TabletCatalog>();
    }
    return catalog;
}

std::shared_ptr<tablet::TabletCatalog> BuildCommonCatalog(
    const fesql::type::TableDef& table_def) {
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    return BuildCommonCatalog(table_def, table);
}


void PrintSchema(std::ostringstream& ss, const Schema& schema) {
    for (int32_t i = 0; i < schema.size(); i++) {
        if (i > 0) {
            ss << "\n";
        }
        const type::ColumnDef& column = schema.Get(i);
        ss << column.name() << " " << type::Type_Name(column.type());
    }
}

void PrintSchema(const Schema& schema) {
    std::ostringstream ss;
    PrintSchema(ss, schema);
    LOG(INFO) << "\n" << ss.str();
}




}  // namespace vm
}  // namespace fesql

#endif  // SRC_VM_TEST_BASE_H_
