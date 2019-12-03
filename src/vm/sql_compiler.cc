/*
 * sql_compiler.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#include "vm/sql_compiler.h"
#include <udf/udf.h>
#include <memory>
#include <utility>
#include <vector>
#include "analyser/analyser.h"
#include "glog/logging.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "vm/op_generator.h"

namespace fesql {
namespace vm {
using ::fesql::base::Status;

SQLCompiler::SQLCompiler(TableMgr* table_mgr) : table_mgr_(table_mgr) {}

SQLCompiler::~SQLCompiler() {}

void SQLCompiler::RegisterUDF(::llvm::Module* m) {
    ::llvm::Type* i32_ty = ::llvm::Type::getInt32Ty(m->getContext());
    ::llvm::Type* i8_ptr_ty = ::llvm::Type::getInt8PtrTy(m->getContext());
    m->getOrInsertFunction("inc_int32", i32_ty, i32_ty);
    m->getOrInsertFunction("sum_int32", i32_ty, i8_ptr_ty);
    m->getOrInsertFunction("col", i8_ptr_ty, i8_ptr_ty, i32_ty, i32_ty, i32_ty);
}
void SQLCompiler::RegisterDyLib(FeSQLJIT* jit, ::llvm::orc::JITDylib& jd) {
    jit->AddSymbol(jd, "inc_int32",
                   reinterpret_cast<void*>(&fesql::udf::inc_int32));
    jit->AddSymbol(jd, "sum_int32",
                   reinterpret_cast<void*>(&fesql::udf::sum_int32));
    jit->AddSymbol(jd, "sum_int64",
                   reinterpret_cast<void*>(&fesql::udf::sum_int64));
    jit->AddSymbol(jd, "col", reinterpret_cast<void*>(&fesql::udf::col));
}
bool SQLCompiler::Compile(SQLContext& ctx, Status& status) {  // NOLINT
    LOG(INFO) << "start to compile sql " << ctx.sql;
    ::fesql::node::NodeManager nm;
    ::fesql::node::PlanNodeList trees;
    bool ok = Parse(ctx, nm, trees, status);
    if (!ok) {
        return false;
    }

    OpGenerator op_generator(table_mgr_);
    auto llvm_ctx = ::llvm::make_unique<::llvm::LLVMContext>();
    auto m = ::llvm::make_unique<::llvm::Module>("sql", *llvm_ctx);

    RegisterUDF(m.get());
    ok = op_generator.Gen(trees, ctx.db, m.get(), &ctx.ops, status);
    // TODO(wangtaize) clean ctx
    if (!ok) {
        LOG(WARNING) << "fail to generate operators for sql: \n" << ctx.sql;
        return false;
    }

    ::llvm::Expected<std::unique_ptr<FeSQLJIT>> jit_expected(
        FeSQLJITBuilder().create());
    if (jit_expected.takeError()) {
        LOG(WARNING) << "fail to init jit let";
        return false;
    }
    ctx.jit = std::move(*jit_expected);
    ::llvm::orc::JITDylib& jd = ctx.jit->createJITDylib("sql");
    ::llvm::orc::VModuleKey key = ctx.jit->CreateVModule();
    ::llvm::Error e =
        ctx.jit->AddIRModule(jd,
                             std::move(::llvm::orc::ThreadSafeModule(
                                 std::move(m), std::move(llvm_ctx))),
                             key);
    if (e) {
        LOG(WARNING) << "fail to add ir module  for sql " << ctx.sql;
        return false;
    }
    RegisterDyLib(ctx.jit.get(), jd);
    std::vector<OpNode*>::iterator it = ctx.ops.ops.begin();
    for (; it != ctx.ops.ops.end(); ++it) {
        OpNode* op_node = *it;
        if (op_node->type == kOpProject) {
            ProjectOp* pop = reinterpret_cast<ProjectOp*>(op_node);
            ::llvm::Expected<::llvm::JITEvaluatedSymbol> symbol(
                ctx.jit->lookup(jd, pop->fn_name));
            if (symbol.takeError()) {
                LOG(WARNING) << "fail to find fn with name  " << pop->fn_name
                             << " for sql:\n"
                             << ctx.sql;
            }
            pop->fn = reinterpret_cast<int8_t*>(symbol->getAddress());
            ctx.schema = pop->output_schema;
            ctx.row_size = pop->output_size;
        }
    }
    return true;
}

bool SQLCompiler::Parse(SQLContext& ctx, ::fesql::node::NodeManager& node_mgr,
                        ::fesql::node::PlanNodeList& plan_trees,  // NOLINT
                        ::fesql::base::Status& status) {          // NOLINT
    ::fesql::node::NodePointVector parser_trees;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::plan::SimplePlanner planer(&node_mgr);

    int ret = parser.parse(ctx.sql, parser_trees, &node_mgr, status);
    if (ret != 0) {
        LOG(WARNING) << "fail to parse sql " << ctx.sql << " with error "
                     << status.msg;
        return false;
    }

    // TODO(chenjing): ADD analyser
    ret = planer.CreatePlanTree(parser_trees, plan_trees, status);
    if (ret != 0) {
        LOG(WARNING) << "Fail create sql plan: " << status.msg;
        return false;
    }

    return true;
}

}  // namespace vm
}  // namespace fesql
