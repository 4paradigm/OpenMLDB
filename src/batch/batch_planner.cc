/*
 * batch_planner.cc
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

#include "batch/batch_planner.h"

#include "base/status.h"
#include "glog/logging.h"
#include "vm/sql_compiler.h"

namespace fesql {
namespace batch {

BatchPlanner::BatchPlanner(const std::shared_ptr<BatchCatalog>& catalog,
                           const std::string& db, const std::string& sql)
    : catalog_(catalog), db_(db), sql_(sql) {}

BatchPlanner::~BatchPlanner() {}

bool BatchPlanner::MakePlan(GraphDesc* graph) {
    vm::SQLCompiler compiler(catalog_, true);
    vm::SQLContext ctx;
    ctx.db = db_;
    ctx.sql = sql_;
    base::Status status;
    bool ok = compiler.Compile(ctx, status);
    if (!ok) {
        return false;
    }
    graph->set_ir(ctx.ir);
    uint64_t id_counter = 0;
    // a simple logic plan to physic plan transform
    if (ctx.ops.size() == 2 && ctx.ops[0].type == vm::kOpScan &&
        ctx.ops[1].type == vm::kOpProject) {
        vm::ScanOP* sop = reinterpret_cast<vm::ScanOP>(ctx.ops[0]);
        // add datasource node
        DataSource ds;
        ds.set_db(sop->db);
        ds.set_name(sop->table_handler->GetName());
        ds.mutable_schema()->CopyFrom(sop->table_handler->GetSchema());
        NodeDesc* ds_node = graph->add_nodes();
        NodeValue* ds_value = graph->add_values();
        ds_value->set_id(id_counter++);
        ds_value->set_type(kDataSource);
        ds.SerializeToString(ds_value->mutable_value());
        ds_node->set_id(ds_value->id());
        ds_node->set_op(PlanOpType_Name(kDataSource));
        // add map node
        vm::ProjectOp* project = reinterpret_cast<vm::ProjectOp>(ctx.ops[1]);
    }
}

}  // namespace batch
}  // namespace fesql
