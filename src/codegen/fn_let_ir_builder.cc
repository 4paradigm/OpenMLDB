/*
 * fn_let_ir_builder.cc
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

#include "codegen/fn_let_ir_builder.h"

#include "codegen/buf_ir_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {

RowFnLetIRBuilder::RowFnLetIRBuilder(::fesql::type::TableDef* table,
        ::llvm::Module* module):table_(table), module_(module) {}

~RowFnLetIRBuilder::RowFnLetIRBuilder() {}

bool RowFnLetIRBuilder::RowFnLetIRBuilder(
        const std::string& name,
        const ::fesql::node::ProjectListPlanNode* node,
        std::vector<::fesql::type::ColumnDef>& schema) {

    if (node == NULL) {
        LOG(WARNING) << "node is null";
        return false;
    }

    ::llvm::Function *fn = NULL;
    ::llvm::StringRef name_ref(name);
    if (module_->getFunction(name_ref) != NULL) {
        LOG(WARNING) << "function with name " << name << " exist";
        return false;
    }

    bool ok = BuildFnHeader(name, &fn);
    if (!ok) {
        LOG(WARNING) << "fail to build fn header for name " <<  name;
        return false;
    }

    ::llvm::BasicBlock *block = ::llvm::BasicBlock::Create(module_->getContext(), "entry", fn);
    const ::fesql::node::PlanNodeList& children = node->GetProjects();
    ::fesql::node::PlanNodeList::const_iterator it = children.begin();
    for (; it != children.end(); i++) {
        const ::fesql::node::PlanNode* pn = *it;
        if (pn == NULL) {
            LOG(WARNING) << "plan node is null";
            continue;
        }

        if (pn->GetType() != ::fesql::node::kProject) {
            LOG(WARNING) << "project node is required but " << ::fesql::node::NameOfPlanNodeType(pn->GetType());
            continue;
        }

    }
}

bool RowFnLetIRBuilder::BuildProject(const ::fesql::node::PlanNode* node,
        ::llvm::BasicBlock* block) {

    if (node == NULL || block == NULL) {
        LOG(WARNING) << "node or block is null" ;
        return false;
    }

    const ::fesql::node::ProjectPlanNode* pp_node =
        (const ::fesql::node::ProjectPlanNode)node;
}

bool RowFnLetIRBuilder::BuildFnHeader(const std::string& name,
        ::llvm::Function **fn) {

    if (fn == NULL) {
        LOG(WARNING) << "fn is null";
        return false;
    }

    // the function input args is two int8 ptr
    std::vector<::llvm::Type*> args_type;
    args_type.push_back(::llvm::type::getInt8PtrTy(module_->getContext()));
    args_type.push_back(::llvm::type::getInt8PtrTy(module_->getContext()));
    ::llvm::ArrayRef<::llvm::Type *> array_ref(args_type);
    ::llvm::FunctionType *fnt = ::llvm::FunctionType::get(::llvm::type::getInt32Ty(module_->getContext()),
            array_ref, 
            false);
     *fn = ::llvm::Function::Create(fnt, 
             ::llvm::Function::Private,
             name, module_);
     return true;
}

}  // namespace codegen
}  // namespace fesql
