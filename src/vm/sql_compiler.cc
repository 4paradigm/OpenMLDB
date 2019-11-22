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

#include "glog/logging.h"
#include "parser/parser.h"
#include "vm/op_generator.h"

namespace fesql {
namespace vm {
using ::fesql::common::Status;

SQLCompiler::SQLCompiler(TableMgr* table_mgr) : table_mgr_(table_mgr) {}

SQLCompiler::~SQLCompiler() {}

bool SQLCompiler::Compile(SQLContext& ctx, Status &status) { //NOLINT
    LOG(INFO) << "start to compile sql " << ctx.sql;
    ::fesql::node::NodeManager nm;
    ::fesql::node::NodePointVector  trees;
    bool ok = Parse(ctx.sql, nm, trees, status);
    if (!ok) {
        return false;
    }
    OpGenerator op_generator(table_mgr_);
    auto llvm_ctx = ::llvm::make_unique<::llvm::LLVMContext>();
    auto m = ::llvm::make_unique<::llvm::Module>("sql", *llvm_ctx);
    ok = op_generator.Gen(trees, ctx.db, m.get(), &ctx.ops, status);
    // TODO(wangtaize) clean ctx
    if (!ok) {
        LOG(WARNING) << "fail to generate operators for sql " << ctx.sql;
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

    std::vector<OpNode*>::iterator it = ctx.ops.ops.begin();
    for (; it != ctx.ops.ops.end(); ++it) {
        OpNode* op_node = *it;
        if (op_node->type == kOpProject) {
            ProjectOp* pop = (ProjectOp*)op_node;
            ::llvm::Expected<::llvm::JITEvaluatedSymbol> symbol(
                ctx.jit->lookup(jd, pop->fn_name));
            if (symbol.takeError()) {
                LOG(WARNING) << "fail to find fn with name  " << pop->fn_name
                             << " for sql" << ctx.sql;
            }
            pop->fn = (int8_t*)symbol->getAddress();
            ctx.schema = pop->output_schema;
            ctx.row_size = pop->output_size;
        }
    }
    return true;
}

bool SQLCompiler::Parse(const std::string& sql,
        ::fesql::node::NodeManager& node_mgr,
        ::fesql::node::NodePointVector& trees, Status &status) {
    ::fesql::parser::FeSQLParser parser;
    ::fesql::base::Status parse_status;
    int ret = parser.parse(sql, trees, &node_mgr, parse_status);
    if (ret != 0) {
        LOG(WARNING) << "fail to parse sql " << sql << " with error " << parse_status.msg;
        status.set_msg(parse_status.msg);
        status.set_code(common::kSQLError);
        return false;
    }
    return true;
}

}  // namespace vm
}  // namespace fesql
