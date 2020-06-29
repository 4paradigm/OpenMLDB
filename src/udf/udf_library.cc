/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_library.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf_library.h"

#include <vector>
#include <unordered_set>
#include "boost/filesystem.hpp"
#include "boost/filesystem/string_file.hpp"

#include "plan/planner.h"
#include "parser/parser.h"
#include "udf/udf_registry.h"
#include "codegen/type_ir_builder.h"

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

template <typename Helper, typename RegistryT>
Helper UDFLibrary::DoStartRegister(const std::string& name) {
    auto reg_item = std::make_shared<RegistryT>(name);
    InsertRegistry(reg_item);
    return Helper(reg_item, this);
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

SimpleUDAFRegistryHelper UDFLibrary::RegisterSimpleUDAF(
    const std::string& name) {
    auto helper =
        DoStartRegister<SimpleUDAFRegistryHelper, SimpleUDAFRegistry>(name);
    return helper;
}


Status UDFLibrary::RegisterAlias(const std::string& alias,
                                 const std::string& name) {
    auto iter = table_.find(alias);
    CHECK_TRUE(iter == table_.end(),
        "Function name '", alias, "' is duplicated");
    iter = table_.find(name);
    CHECK_TRUE(iter != table_.end(),
        "Alias target Function name '", name, "' not found");
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
                    registry = std::make_shared<SimpleUDFRegistry>(
                        header->name_);
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
                return Status(common::kCodegenError,
                    "fail to codegen fe script: unrecognized plan type " +
                    node::NameOfPlanNodeType(node->GetType()));
        }
    }
    for (auto& pair : dict) {
        InsertRegistry(pair.second);
    }
    return Status::OK();
}


Status UDFLibrary::Transform(const std::string& name, ExprListNode* args,
                             const node::SQLNode* over,
                             node::NodeManager* manager,
                             ExprNode** result) const {
    UDFResolveContext ctx(args, over, manager);
    return this->Transform(name, &ctx, result);
}

Status UDFLibrary::Transform(const std::string& name, UDFResolveContext* ctx,
                             ExprNode** result) const {
    auto iter = table_.find(name);
    CHECK_TRUE(iter != table_.end(),
               "Fail to find registered function: ", name);
    return iter->second->Transform(ctx, result);
}

void UDFLibrary::InitJITSymbols(llvm::orc::LLJIT* jit_ptr) {
    ::llvm::orc::MangleAndInterner mi(jit_ptr->getExecutionSession(),
                                      jit_ptr->getDataLayout());
    std::unordered_set<std::string> symbol_names;
    for (auto& pair : table_) {
        for (auto registry : pair.second->GetSubRegistries()) {
            auto extern_reg =
                std::dynamic_pointer_cast<ExternalFuncRegistry>(registry);
            if (extern_reg == nullptr) {
                continue;
            }
            for (auto& pair2 : extern_reg->GetTable().GetTable()) {
                node::ExternalFnDefNode* def_node = pair2.second.first;
                if (def_node->IsResolved() &&
                    def_node->function_ptr() != nullptr &&
                    symbol_names.find(def_node->function_name())
                        == symbol_names.end()) {
                    fesql::vm::FeSQLJIT::AddSymbol(
                        jit_ptr->getMainJITDylib(), mi,
                        def_node->function_name(), def_node->function_ptr());
                    symbol_names.insert(def_node->function_name());
                }
            }
        }
    }
}


}  // namespace udf
}  // namespace fesql
