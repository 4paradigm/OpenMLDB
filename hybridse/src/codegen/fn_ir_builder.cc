/*
 * Copyright 2021 4Paradigm
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

#include "codegen/fn_ir_builder.h"
#include <stack>
#include <string>
#include "codegen/block_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/type_ir_builder.h"
#include "codegen/variable_ir_builder.h"
#include "glog/logging.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/IR/CFG.h"
#include "llvm/IR/IRBuilder.h"
#include "node/node_manager.h"
#include "passes/resolve_udf_def.h"

namespace hybridse {
namespace codegen {

FnIRBuilder::FnIRBuilder(::llvm::Module *module) : module_(module) {}

FnIRBuilder::~FnIRBuilder() {}

bool FnIRBuilder::Build(::hybridse::node::FnNodeFnDef *root,
                        ::llvm::Function **result,
                        base::Status &status) {  // NOLINT
    if (root == NULL || root->GetType() != ::hybridse::node::kFnDef) {
        status.code = common::kCodegenError;
        status.msg = "node is null";
        LOG(WARNING) << status;
        return false;
    }

    passes::ResolveUdfDef resolver;
    status = resolver.Visit(root);
    if (!status.isOK()) {
        LOG(WARNING) << "Fail to resolve udf function: " << status;
        return false;
    }

    node::NodeManager nm;
    vm::SchemasContext empty_schema;
    CodeGenContext ctx(module_, &empty_schema, nullptr, &nm);

    ::llvm::Function *fn = NULL;
    const ::hybridse::node::FnNodeFnHeander *fn_def = root->header_;

    bool ok = BuildFnHead(fn_def, &ctx, &fn, status);
    if (!ok) {
        return false;
    }

    FunctionScopeGuard fn_guard(fn, &ctx);
    BlockIRBuilder block_ir_builder(&ctx);
    if (false == block_ir_builder.BuildBlock(root->block_, status)) {
        return false;
    }

    // reformat scope
    auto root_scope = ctx.GetCurrentScope();
    root_scope->blocks()->DropEmptyBlocks();
    root_scope->blocks()->ReInsertTo(fn);

    *result = fn;
    return true;
}

bool FnIRBuilder::BuildFnHead(const ::hybridse::node::FnNodeFnHeander *header,
                              CodeGenContextBase *ctx, ::llvm::Function **fn,
                              base::Status &status) {  // NOLINE
    ::llvm::Type *ret_type = NULL;
    bool ok = GetLlvmType(module_, header->ret_type_, &ret_type);
    if (!ok) {
        status.code = common::kCodegenError;
        status.msg = "fail to get llvm type";
        return false;
    }

    bool return_by_arg = TypeIRBuilder::IsStructPtr(ret_type);

    if (!CreateFunction(header, return_by_arg, fn, status)) {
        LOG(WARNING) << "Fail Build Function Header: " << status;
        return false;
    }

    FunctionScopeGuard fn_guard(*fn, ctx);
    ScopeVar *sv = ctx->GetCurrentScope()->sv();
    if (header->parameters_) {
        bool ok = FillArgs(header->parameters_, sv, return_by_arg, *fn, status);
        if (!ok) {
            return false;
        }
    }
    return true;
}

bool FnIRBuilder::CreateFunction(
    const ::hybridse::node::FnNodeFnHeander *fn_def, bool return_by_arg,
    ::llvm::Function **fn,
    base::Status &status) {  // NOLINE
    if (fn_def == NULL || fn == NULL) {
        status.code = common::kCodegenError;
        status.msg = "input is null";
        LOG(WARNING) << status;
        return false;
    }

    ::llvm::Type *ret_type = NULL;
    bool ok = GetLlvmType(module_, fn_def->ret_type_, &ret_type);
    if (!ok) {
        status.code = common::kCodegenError;
        status.msg = "fail to get llvm type";
        return false;
    }
    std::vector<::llvm::Type *> paras;
    if (nullptr != fn_def->parameters_) {
        bool ok = BuildParas(fn_def->parameters_, paras, status);
        if (!ok) {
            return false;
        }
    }
    if (return_by_arg) {
        paras.push_back(ret_type);
        ret_type = ::llvm::Type::getInt1Ty(module_->getContext());
    }

    std::string fn_name = fn_def->GeIRFunctionName();
    ::llvm::ArrayRef<::llvm::Type *> array_ref(paras);
    ::llvm::FunctionType *fnt =
        ::llvm::FunctionType::get(ret_type, array_ref, false);
    auto callee = module_->getOrInsertFunction(fn_name, fnt);
    *fn = reinterpret_cast<::llvm::Function *>(callee.getCallee());
    return true;
}

bool FnIRBuilder::FillArgs(const ::hybridse::node::FnNodeList *node,
                           ScopeVar *sv, bool return_by_arg,
                           ::llvm::Function *fn,
                           base::Status &status) {  // NOLINE
    if (node == NULL) {
        status.code = common::kCodegenError;
        status.msg = "node is null or node type mismatch";
        LOG(WARNING) << status;
        return false;
    }

    ::llvm::Function::arg_iterator it = fn->arg_begin();
    uint32_t index = 0;
    for (; it != fn->arg_end() && index < node->children.size(); ++it) {
        ::hybridse::node::FnParaNode *pnode =
            static_cast<::hybridse::node::FnParaNode *>(node->children[index]);
        ::llvm::Argument *argu = &*it;

        bool ok = sv->AddVar(pnode->GetExprId()->GetExprString(),
                             NativeValue::Create(argu));
        if (!ok) {
            status.code = common::kCodegenError;
            status.msg = "fail to define var " + pnode->GetName();
            LOG(WARNING) << status;
            return false;
        }
        index++;
    }
    if (return_by_arg) {
        // last llvm func arg is ret addr
        bool ok = sv->AddVar("@ret_struct", NativeValue::Create(&*it));
        if (!ok) {
            status.code = common::kCodegenError;
            status.msg = "fail to define @ret_struct";
            LOG(WARNING) << status;
            return false;
        }
    }
    return true;
}

bool FnIRBuilder::BuildParas(const ::hybridse::node::FnNodeList *node,
                             std::vector<::llvm::Type *> &paras,
                             base::Status &status) {  // NOLINE
    if (node == NULL) {
        status.code = common::kCodegenError;
        status.msg = "node is null or node type mismatch";
        LOG(WARNING) << status;
        return false;
    }

    for (uint32_t i = 0; i < node->children.size(); i++) {
        ::hybridse::node::FnParaNode *pnode =
            static_cast<::hybridse::node::FnParaNode *>(node->children[i]);
        ::llvm::Type *type = NULL;
        bool ok = GetLlvmType(module_, pnode->GetParaType(), &type);
        if (!ok) {
            status.code = common::kCodegenError;
            status.msg =
                "fail to get primary type for pname " + pnode->GetName();
            LOG(WARNING) << status;
            return false;
        }
        paras.push_back(type);
    }
    return true;
}

}  // namespace codegen
}  // namespace hybridse
