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

#ifndef HYBRIDSE_SRC_CODEGEN_SCOPE_VAR_H_
#define HYBRIDSE_SRC_CODEGEN_SCOPE_VAR_H_

#include <map>
#include <string>
#include <utility>
#include <vector>
#include "codegen/native_value.h"
#include "llvm/IR/IRBuilder.h"

namespace hybridse {
namespace codegen {

class ScopeVar {
 public:
    ScopeVar();
    explicit ScopeVar(ScopeVar* parent);
    ~ScopeVar();

    bool AddVar(const std::string& name, const NativeValue& value);
    bool ReplaceVar(const std::string& name, const NativeValue& value);
    bool FindVar(const std::string& name, NativeValue* value);
    bool HasVar(const std::string& name);

    // Register values to be destroyed before exit scope
    bool AddIteratorValue(::llvm::Value* value);
    std::vector<const std::vector<::llvm::Value*>*> GetIteratorValues();
    const std::vector<::llvm::Value*>* GetScopeIteratorValues();

    ScopeVar* parent() const { return parent_; }
    void SetParent(ScopeVar* parent) { parent_ = parent; }

 private:
    ScopeVar* parent_;
    std::map<std::string, NativeValue> scope_map_;
    std::vector<::llvm::Value*> scope_iterators_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_SCOPE_VAR_H_
