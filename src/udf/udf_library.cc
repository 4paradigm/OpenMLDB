/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_library.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf_library.h"

#include <unordered_set>
#include <vector>
#include "boost/filesystem.hpp"
#include "boost/filesystem/string_file.hpp"

#include "codegen/type_ir_builder.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "udf/udf_registry.h"

namespace fesql {
namespace udf {

std::shared_ptr<UDFTransformRegistry> UDFLibrary::Find(
    const std::string& name) const {
    auto iter = table_.find(name);
    if (iter == table_.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

void UDFLibrary::InsertRegistry(
    std::shared_ptr<UDFTransformRegistry> reg_item) {
    auto name = reg_item->name();
    std::shared_ptr<CompositeRegistry> composite_registry;
    auto iter = table_.find(name);
    if (iter == table_.end()) {
        composite_registry = std::make_shared<CompositeRegistry>(name);
        table_.insert(iter, std::make_pair(name, composite_registry));
    } else {
        composite_registry = iter->second;
    }
    composite_registry->Add(reg_item);
}

bool UDFLibrary::IsUDAF(const std::string& name, size_t args) {
    auto iter = udaf_tags_.find(name);
    if (iter == udaf_tags_.end()) {
        return false;
    }
    auto& arg_num_set = iter->second;
    return arg_num_set.find(args) != arg_num_set.end();
}

void UDFLibrary::SetIsUDAF(const std::string& name, size_t args) {
    udaf_tags_[name].insert(args);
}

ExprUDFRegistryHelper UDFLibrary::RegisterExprUDF(const std::string& name) {
    return DoStartRegister<ExprUDFRegistryHelper, ExprUDFRegistry>(name);
}

LLVMUDFRegistryHelper UDFLibrary::RegisterCodeGenUDF(const std::string& name) {
    return DoStartRegister<LLVMUDFRegistryHelper, LLVMUDFRegistry>(name);
}

ExternalFuncRegistryHelper UDFLibrary::RegisterExternal(
    const std::string& name) {
    return DoStartRegister<ExternalFuncRegistryHelper, ExternalFuncRegistry>(
        name);
}

UDAFRegistryHelper UDFLibrary::RegisterUDAF(const std::string& name) {
    auto helper = DoStartRegister<UDAFRegistryHelper, UDAFRegistry>(name);
    return helper;
}

Status UDFLibrary::RegisterAlias(const std::string& alias,
                                 const std::string& name) {
    auto iter = table_.find(alias);
    CHECK_TRUE(iter == table_.end(), "Function name '", alias,
               "' is duplicated");
    iter = table_.find(name);
    CHECK_TRUE(iter != table_.end(), "Alias target Function name '", name,
               "' not found");
    table_[alias] = iter->second;
    return Status::OK();
}

Status UDFLibrary::RegisterFromFile(const std::string& path_str) {
    boost::filesystem::path path(path_str);
    std::string script;
    boost::filesystem::load_string_file(path, script);
    DLOG(INFO) << "Script file : " << script << "\n" << script;

    ::fesql::node::NodePointVector parser_trees;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::plan::SimplePlanner planer(node_manager());
    ::fesql::node::PlanNodeList plan_trees;

    Status status;
    CHECK_TRUE(0 == parser.parse(script, parser_trees, node_manager(), status),
               "Fail to parse script:", status.msg);
    CHECK_TRUE(0 == planer.CreatePlanTree(parser_trees, plan_trees, status),
               "Fail to create sql plan: ", status.msg);

    std::unordered_map<std::string, std::shared_ptr<SimpleUDFRegistry>> dict;
    auto it = plan_trees.begin();
    for (; it != plan_trees.end(); ++it) {
        const ::fesql::node::PlanNode* node = *it;
        CHECK_TRUE(node != nullptr, "Compile null plan");
        switch (node->GetType()) {
            case ::fesql::node::kPlanTypeFuncDef: {
                auto func_def_plan =
                    dynamic_cast<const ::fesql::node::FuncDefPlanNode*>(node);
                CHECK_TRUE(func_def_plan->fn_def_ != nullptr,
                           "fn_def node is null");

                auto header = func_def_plan->fn_def_->header_;
                std::shared_ptr<SimpleUDFRegistry> registry;
                auto iter = dict.find(header->name_);
                if (iter == dict.end()) {
                    registry =
                        std::make_shared<SimpleUDFRegistry>(header->name_);
                    dict.insert(iter, std::make_pair(header->name_, registry));
                } else {
                    registry = iter->second;
                }

                auto def_node = dynamic_cast<node::UDFDefNode*>(
                    node_manager()->MakeUDFDefNode(func_def_plan->fn_def_));
                std::vector<std::string> arg_types;
                for (size_t i = 0; i < def_node->GetArgSize(); ++i) {
                    arg_types.push_back(def_node->GetArgType(i)->GetName());
                }
                registry->Register(arg_types, def_node);
                break;
            }
            default:
                return Status(
                    common::kCodegenError,
                    "fail to codegen fe script: unrecognized plan type " +
                        node::NameOfPlanNodeType(node->GetType()));
        }
    }
    for (auto& pair : dict) {
        InsertRegistry(pair.second);
    }
    return Status::OK();
}

Status UDFLibrary::Transform(const std::string& name,
                             const std::vector<node::ExprNode*>& args,
                             node::NodeManager* node_manager,
                             ExprNode** result) {
    UDFResolveContext ctx(args, node_manager, this);
    return this->Transform(name, &ctx, result);
}

Status UDFLibrary::Transform(const std::string& name, UDFResolveContext* ctx,
                             ExprNode** result) {
    auto iter = table_.find(name);
    CHECK_TRUE(iter != table_.end(),
               "Fail to find registered function: ", name);
    return iter->second->Transform(ctx, result);
}

Status UDFLibrary::ResolveFunction(const std::string& name,
                                   UDFResolveContext* ctx,
                                   node::FnDefNode** result) {
    auto iter = table_.find(name);
    CHECK_TRUE(iter != table_.end(),
               "Fail to find registered function: ", name);
    return iter->second->ResolveFunction(ctx, result);
}

Status UDFLibrary::ResolveFunction(const std::string& name,
                                   const std::vector<node::ExprNode*>& args,
                                   node::NodeManager* node_manager,
                                   node::FnDefNode** result) {
    UDFResolveContext ctx(args, node_manager, this);
    return this->ResolveFunction(name, &ctx, result);
}

void UDFLibrary::AddExternalSymbol(const std::string& name, void* addr) {
    external_symbols_.insert(std::make_pair(name, addr));
}

void UDFLibrary::InitJITSymbols(llvm::orc::LLJIT* jit_ptr) {
    ::llvm::orc::MangleAndInterner mi(jit_ptr->getExecutionSession(),
                                      jit_ptr->getDataLayout());
    for (auto& pair : external_symbols_) {
        fesql::vm::FeSQLJIT::AddSymbol(jit_ptr->getMainJITDylib(), mi,
                                       pair.first, pair.second);
    }
}

}  // namespace udf
}  // namespace fesql
