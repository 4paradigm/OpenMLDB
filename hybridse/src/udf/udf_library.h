/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_SRC_UDF_UDF_LIBRARY_H_
#define HYBRIDSE_SRC_UDF_UDF_LIBRARY_H_

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/fe_status.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "udf/dynamic_lib_manager.h"

namespace hybridse {

namespace vm {
class HybridSeJitWrapper;
}

namespace udf {

using hybridse::base::Status;
using hybridse::node::ExprNode;

// Forward declarations
class ExprUdfRegistryHelper;
class LlvmUdfRegistryHelper;
class ExternalFuncRegistryHelper;
class UdafRegistryHelper;
class UdfRegistry;
class UdafRegistry;
class CompositeRegistry;
class UdfResolveContext;

class ArgSignatureTable;

template <template <typename> typename FTemplate>
class ExternalTemplateFuncRegistryHelper;

template <template <typename> typename FTemplate>
class CodeGenUdfTemplateRegistryHelper;

template <template <typename> typename FTemplate>
class UdafTemplateRegistryHelper;

template <template <typename> typename FTemplate>
class ExprUdfTemplateRegistryHelper;

struct UdfLibraryEntry;

/**
 * Hold global udf registry entries.
 * "fn(arg0, arg1, ...argN)" -> some expression
 */
class UdfLibrary {
 public:
    UdfLibrary() = default;
    UdfLibrary(const UdfLibrary&) = delete;
    virtual ~UdfLibrary() {}

    UdfLibrary& operator=(const UdfLibrary&) = delete;

    Status Transform(const std::string& name,
                     const std::vector<node::ExprNode*>& args,
                     node::NodeManager* node_manager, ExprNode** result) const;

    Status Transform(const std::string& name, UdfResolveContext* ctx,
                     ExprNode** result) const;

    Status ResolveFunction(const std::string& name, UdfResolveContext* ctx,
                           node::FnDefNode** result) const;

    Status ResolveFunction(const std::string& name,
                           const std::vector<node::ExprNode*>& args,
                           node::NodeManager* node_manager,
                           node::FnDefNode** result) const;

    std::shared_ptr<UdfRegistry> Find(
        const std::string& name,
        const std::vector<const node::TypeNode*>& arg_types) const;

    bool HasFunction(const std::string& name) const;

    std::shared_ptr<ArgSignatureTable> FindAll(const std::string& name) const;

    bool IsUdaf(const std::string& name, size_t args) const;
    bool IsUdaf(const std::string& name) const;
    void SetIsUdaf(const std::string& name, size_t args);

    bool RequireListAt(const std::string& name, size_t index) const;
    bool IsListReturn(const std::string& name) const;

    Status RegisterDynamicUdf(const std::string& name, node::DataType return_type,
            const std::vector<node::DataType>& arg_types, bool is_aggregate, const std::string& file);

    Status RemoveDynamicUdf(const std::string& name, const std::vector<node::DataType>& arg_types,
            const std::string& file);

    // register interfaces
    ExprUdfRegistryHelper RegisterExprUdf(const std::string& name);
    LlvmUdfRegistryHelper RegisterCodeGenUdf(const std::string& name);
    ExternalFuncRegistryHelper RegisterExternal(const std::string& name);
    UdafRegistryHelper RegisterUdaf(const std::string& name);

    Status RegisterAlias(const std::string& alias, const std::string& name);
    Status RegisterFromFile(const std::string& path);

    template <template <typename> class FTemplate>
    auto RegisterExternalTemplate(const std::string& name) {
        return ExternalTemplateFuncRegistryHelper<FTemplate>(name, this);
    }

    template <template <typename> class FTemplate>
    auto RegisterCodeGenUdfTemplate(const std::string& name) {
        return CodeGenUdfTemplateRegistryHelper<FTemplate>(name, this);
    }

    template <template <typename> class FTemplate>
    auto RegisterUdafTemplate(const std::string& name) {
        return UdafTemplateRegistryHelper<FTemplate>(name, this);
    }

    template <template <typename> class FTemplate>
    auto RegisterExprUdfTemplate(const std::string& name) {
        return ExprUdfTemplateRegistryHelper<FTemplate>(name, this);
    }

    void AddExternalFunction(const std::string& name, void* addr);

    void InitJITSymbols(vm::HybridSeJitWrapper* jit_ptr);

    node::NodeManager* node_manager() { return &nm_; }

    std::unordered_map<std::string, std::shared_ptr<UdfLibraryEntry>> GetAllRegistries() {
        std::lock_guard<std::mutex> lock(mu_);
        return table_;
    }

    void InsertRegistry(const std::string& name,
                        const std::vector<const node::TypeNode*>& arg_types,
                        bool is_variadic, bool always_return_list,
                        const std::unordered_set<size_t>& always_list_argidx,
                        std::shared_ptr<UdfRegistry> registry);

 private:
    std::string GetCanonicalName(const std::string& name) const;

    // argument type matching table
    std::unordered_map<std::string, std::shared_ptr<UdfLibraryEntry>> table_;

    // external symbols
    std::unordered_map<std::string, void*> external_symbols_;

    node::NodeManager nm_;

    DynamicLibManager lib_manager_;

    const bool case_sensitive_ = false;
    mutable std::mutex mu_;
};

const std::string GetArgSignature(const std::vector<node::ExprNode*>& args);

}  // namespace udf
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_UDF_UDF_LIBRARY_H_
