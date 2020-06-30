/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_library.h
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_UDF_UDF_LIBRARY_H_
#define SRC_UDF_UDF_LIBRARY_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "base/fe_status.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "vm/jit.h"

namespace fesql {
namespace udf {

using fesql::base::Status;
using fesql::node::ExprListNode;
using fesql::node::ExprNode;
using fesql::node::SQLNode;

// Forward declarations
class ExprUDFRegistryHelper;
class LLVMUDFRegistryHelper;
class ExternalFuncRegistryHelper;
class SimpleUDAFRegistryHelper;
class UDFTransformRegistry;
class CompositeRegistry;
class UDFResolveContext;

template <template <typename> typename FTemplate>
class ExternalTemplateFuncRegistryHelper;

template <template <typename> typename FTemplate>
class CodeGenUDFTemplateRegistryHelper;

/**
 * Hold global udf registry entries.
 * "fn(arg0, arg1, ...argN)" -> some expression
 */
class UDFLibrary {
 public:
    Status Transform(const std::string& name, ExprListNode* args,
                     const node::SQLNode* over, node::NodeManager* manager,
                     ExprNode** result) const;

    Status Transform(const std::string& name, UDFResolveContext* ctx,
                     ExprNode** result) const;

    std::shared_ptr<UDFTransformRegistry> Find(const std::string& name) const;

    // register interfaces
    ExprUDFRegistryHelper RegisterExprUDF(const std::string& name);
    LLVMUDFRegistryHelper RegisterCodeGenUDF(const std::string& name);
    ExternalFuncRegistryHelper RegisterExternal(const std::string& name);
    SimpleUDAFRegistryHelper RegisterSimpleUDAF(const std::string& name);

    Status RegisterAlias(const std::string& alias, const std::string& name);
    Status RegisterFromFile(const std::string& path);

    template <template <typename> class FTemplate>
    auto RegisterExternalTemplate(const std::string& name) {
        return ExternalTemplateFuncRegistryHelper<FTemplate>(name, this);
    }

    template <template <typename> class FTemplate>
    auto RegisterCodeGenUDFTemplate(const std::string& name) {
        return CodeGenUDFTemplateRegistryHelper<FTemplate>(name, this);
    }

    void InitJITSymbols(::llvm::orc::LLJIT* jit_ptr);

    node::NodeManager* node_manager() { return &nm_; }

 private:
    template <typename Helper, typename RegistryT>
    Helper DoStartRegister(const std::string& name);

    void InsertRegistry(std::shared_ptr<UDFTransformRegistry> reg_item);

    std::unordered_map<std::string, std::shared_ptr<CompositeRegistry>> table_;

    node::NodeManager nm_;
};

}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_UDF_LIBRARY_H_
