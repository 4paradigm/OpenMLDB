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

#ifndef HYBRIDSE_SRC_CODEGEN_FN_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_FN_IR_BUILDER_H_

#include <vector>
#include "base/fe_status.h"
#include "codegen/scope_var.h"
#include "llvm/IR/Module.h"
#include "node/sql_node.h"

namespace hybridse {
namespace codegen {

// FnIRBuilder
class FnIRBuilder {
 public:
    // TODO(wangtaize) provide a module manager
    explicit FnIRBuilder(::llvm::Module* module);
    ~FnIRBuilder();
    bool Build(::hybridse::node::FnNodeFnDef* node, ::llvm::Function** result,
               base::Status& status);  // NOLINT

    bool CreateFunction(const ::hybridse::node::FnNodeFnHeander* fn_def,
                        bool return_by_arg, ::llvm::Function** fn,
                        base::Status& status);  // NOLINT
    bool BuildFnHead(const ::hybridse::node::FnNodeFnHeander* fn_def,
                     CodeGenContextBase* ctx, ::llvm::Function** fn,
                     base::Status& status);  // NOLINT

 private:
    bool BuildParas(const ::hybridse::node::FnNodeList* node,
                    std::vector<::llvm::Type*>& paras,  // NOLINT
                    base::Status& status);              // NOLINT

    bool FillArgs(const ::hybridse::node::FnNodeList* node, ScopeVar* sv,
                  bool return_by_arg, ::llvm::Function* fn,
                  base::Status& status);  // NOLINT
    ::llvm::Module* module_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_FN_IR_BUILDER_H_
