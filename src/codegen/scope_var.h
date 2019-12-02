/*
 * scope_var.h
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

#ifndef SRC_CODEGEN_SCOPE_VAR_H_
#define SRC_CODEGEN_SCOPE_VAR_H_

#include <map>
#include <string>
#include <vector>
#include "llvm/IR/IRBuilder.h"

namespace fesql {
namespace codegen {

struct Scope {
    std::string name;
    // the value is the pointer or  value
    std::map<std::string, ::llvm::Value*> scope_map;
};

typedef std::vector<Scope> Scopes;

class ScopeVar {
 public:
    ScopeVar();
    ~ScopeVar();

    bool Enter(const std::string& name);

    bool Exit();
    bool AddVar(const std::string& name, ::llvm::Value*);
    bool FindVar(const std::string& name, ::llvm::Value** value);

 private:
    Scopes scopes_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_SCOPE_VAR_H_
