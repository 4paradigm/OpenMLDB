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

#include <memory>
#include <utility>
#include <vector>
#include "analyser/analyser.h"
#include "codegen/ir_base_builder.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/Support/raw_ostream.h"
#include "glog/logging.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "storage/type_native_fn.h"
#include "udf/udf.h"
#include "vm/op_generator.h"

namespace fesql {
namespace vm {
using ::fesql::base::Status;

SQLCompiler::SQLCompiler(const std::shared_ptr<Catalog>& cl,
        bool keep_ir) : cl_(cl), keep_ir_(keep_ir){}

SQLCompiler::~SQLCompiler() {}


void SQLCompiler::KeepIR(SQLContext& ctx, llvm::Module* m) {
    if (m == NULL) {
        LOG(WARNING) << "module is null";
        return;
    }
    ctx.ir.reserve(1024);
    llvm::raw_string_ostream buf(ctx.ir);
    llvm::WriteBitcodeToFile(*m, buf);
    buf.flush();
}

bool SQLCompiler::Compile(SQLContext& ctx, Status& status) {  // NOLINT
    DLOG(INFO) << "start to compile sql " << ctx.sql;
    ::fesql::node::NodeManager nm;
    ::fesql::node::PlanNodeList trees;
    bool ok = Parse(ctx, nm, trees, status);
    if (!ok) {
        return false;
    }
    OpGenerator op_generator(cl_);
    auto llvm_ctx = ::llvm::make_unique<::llvm::LLVMContext>();
    auto m = ::llvm::make_unique<::llvm::Module>("sql", *llvm_ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    ok = op_generator.Gen(trees, ctx.db, m.get(), &ctx.ops, status);
    // TODO(wangtaize) clean ctx
    if (!ok) {
        LOG(WARNING) << "fail to generate operators for sql: \n" << ctx.sql;
        return false;
    }

    ::llvm::Expected<std::unique_ptr<FeSQLJIT>> jit_expected(
        FeSQLJITBuilder().create());
    {
        ::llvm::Error e = jit_expected.takeError();
        if (e) {
            status.msg = "fail to init jit let";
            status.code = common::kJitError;
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    ctx.jit = std::move(*jit_expected);
    ctx.jit->Init();
    if (false == ctx.jit->OptModule(m.get())) {
        LOG(WARNING) << "fail to opt ir module for sql " << ctx.sql;
        return false;
    }

    if (keep_ir_) {
        KeepIR(ctx, m.get());
    }

    ::llvm::Error e = ctx.jit->addIRModule(std::move(
        ::llvm::orc::ThreadSafeModule(std::move(m), std::move(llvm_ctx))));
    if (e) {
        LOG(WARNING) << "fail to add ir module  for sql " << ctx.sql;
        return false;
    }

    storage::InitCodecSymbol(ctx.jit.get());
    udf::InitUDFSymbol(ctx.jit.get());
    std::vector<OpNode*>::iterator it = ctx.ops.ops.begin();
    for (; it != ctx.ops.ops.end(); ++it) {
        OpNode* op_node = *it;
        if (op_node->type == kOpProject) {
            ProjectOp* pop = reinterpret_cast<ProjectOp*>(op_node);
            ::llvm::Expected<::llvm::JITEvaluatedSymbol> symbol(
                ctx.jit->lookup(pop->fn_name));
            if (symbol.takeError()) {
                LOG(WARNING) << "fail to find fn with name  " << pop->fn_name
                             << " for sql:\n"
                             << ctx.sql;
            }
            pop->fn = reinterpret_cast<int8_t*>(symbol->getAddress());
            ctx.schema = pop->output_schema;
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
