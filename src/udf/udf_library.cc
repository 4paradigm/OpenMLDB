/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_library.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf_library.h"
#include "udf/udf_registry.h"

#include <sstream>

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
    std::shared_ptr<CompositeRegistry> composite_registry;
    auto iter = table_.find(name);
    if (iter == table_.end()) {
        composite_registry = std::make_shared<CompositeRegistry>(name);
        table_.insert(iter, std::make_pair(name, composite_registry));
    } else {
        composite_registry = iter->second;
    }
    composite_registry->Add(reg_item);
    return Helper(reg_item, this);
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

void UDFLibrary::RegisterAlias(const std::string& alias,
                               const std::string& name) {
    auto iter = table_.find(alias);
    if (iter != table_.end()) {
        LOG(WARNING) << "Function name '" << alias << "' is duplicated";
        return;
    }
    iter = table_.find(name);
    if (iter == table_.end()) {
        LOG(WARNING) << "Alias target Function name '" << name << "' not found";
        return;
    }
    table_[alias] = iter->second;
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
                    def_node->function_ptr() != nullptr) {
                    fesql::vm::FeSQLJIT::AddSymbol(
                        jit_ptr->getMainJITDylib(), mi,
                        def_node->function_name(), def_node->function_ptr());
                }
            }
        }
    }
}

}  // namespace udf
}  // namespace fesql
