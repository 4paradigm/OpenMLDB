/*
 * scope_var.cc
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

#include "codegen/scope_var.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {
ScopeVar::ScopeVar() {}
ScopeVar::~ScopeVar() {}

bool ScopeVar::Enter(const std::string& name) {
    LOG(INFO) << "enter scope " << name;
    Scope scope;
    scope.name = name;
    if (scopes_.size() <= 0) {
        scopes_.push_back(scope);
        return true;
    }
    // TODO(wangtaize): scope redifine check all scope names
    Scope& exist_scope = scopes_.back();
    if (exist_scope.name.compare(name) == 0) {
        LOG(WARNING) << "redefine scope " << name;
        return false;
    }
    scopes_.push_back(scope);
    return true;
}

bool ScopeVar::Exit() {
    if (scopes_.size() <= 0) {
        return false;
    }
    scopes_.pop_back();
    return true;
}

bool ScopeVar::AddVar(const std::string& name, ::llvm::Value* value) {
    if (scopes_.size() <= 0) {
        LOG(WARNING) << "no scope exists " << name;
        return false;
    }
    Scope& exist_scope = scopes_.back();
    std::map<std::string, ::llvm::Value*>::iterator it =
        exist_scope.scope_map.find(name);
    if (it != exist_scope.scope_map.end()) {
        LOG(WARNING) << "var with name " << name << " exists ";
        return false;
    }
    exist_scope.scope_map.insert(std::make_pair(name, value));
    return true;
}

bool ScopeVar::FindVar(const std::string& name, ::llvm::Value** value) {
    if (value == NULL) {
        LOG(WARNING) << " input value is null";
        return false;
    }

    if (scopes_.size() <= 0) {
        LOG(WARNING) << "no scope exists " << name;
        return false;
    }

    Scope& exist_scope = scopes_.back();
    std::map<std::string, ::llvm::Value*>::iterator it =
        exist_scope.scope_map.find(name);

    if (it == exist_scope.scope_map.end()) {
        LOG(WARNING) << "var with name " << name << " does not exist ";
        return false;
    }

    *value = it->second;
    return true;
}

}  // namespace codegen
}  // namespace fesql
