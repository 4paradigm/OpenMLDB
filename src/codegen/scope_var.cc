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
#include <utility>
#include "glog/logging.h"

namespace fesql {
namespace codegen {
ScopeVar::ScopeVar() {}
ScopeVar::~ScopeVar() {}

bool ScopeVar::Enter(const std::string& name) {
    DLOG(INFO) << "enter scope " << name;
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

bool ScopeVar::AddIteratorValue(::llvm::Value* value) {
    if (scopes_.size() <= 0) {
        LOG(WARNING) << "no scope exists ";
        return false;
    }
    Scope& exist_scope = scopes_.back();
    exist_scope.scope_iterators.push_back(value);
    return true;
}

bool ScopeVar::AddVar(const std::string& name, const NativeValue& value) {
    if (scopes_.size() <= 0) {
        LOG(WARNING) << "no scope exists " << name;
        return false;
    }
    Scope& exist_scope = scopes_.back();
    std::map<std::string, NativeValue>::iterator it =
        exist_scope.scope_map.find(name);
    if (it != exist_scope.scope_map.end()) {
        LOG(WARNING) << "var with name " << name << " exists ";
        return false;
    }
    exist_scope.scope_map.insert(std::make_pair(name, value));
    DLOG(INFO) << "store var " << name;
    return true;
}

bool ScopeVar::ReplaceVar(const std::string& name, const NativeValue& value) {
    if (scopes_.size() <= 0) {
        LOG(WARNING) << "no scope exists " << name;
        return false;
    }

    for (auto scope_iter = scopes_.rbegin(); scope_iter != scopes_.rend();
         scope_iter++) {
        Scope& exist_scope = *scope_iter;
        std::map<std::string, NativeValue>::iterator it =
            exist_scope.scope_map.find(name);
        if (it != exist_scope.scope_map.end()) {
            it->second = value;
            return true;
        }
    }
    DLOG(INFO) << "var with name " << name << " does not exist ";
    return false;
}
bool ScopeVar::FindVar(const std::string& name, NativeValue* value) {
    if (value == NULL) {
        LOG(WARNING) << " input value is null";
        return false;
    }

    if (scopes_.size() <= 0) {
        LOG(WARNING) << "no scope exists " << name;
        return false;
    }

    for (auto scope_iter = scopes_.rbegin(); scope_iter != scopes_.rend();
         scope_iter++) {
        Scope& exist_scope = *scope_iter;
        std::map<std::string, NativeValue>::iterator it =
            exist_scope.scope_map.find(name);
        if (it != exist_scope.scope_map.end()) {
            *value = it->second;
            return true;
        }
    }
    DLOG(INFO) << "var with name " << name << " does not exist ";
    return false;
}
std::vector<const std::vector<::llvm::Value*>*> ScopeVar::GetIteratorValues() {
    std::vector<const std::vector<::llvm::Value*>*> values;
    if (scopes_.size() <= 0) {
        LOG(WARNING) << "no scope exists ";
        return values;
    }
    for (auto iter = scopes_.cbegin(); iter != scopes_.cend(); iter++) {
        values.push_back(&(iter->scope_iterators));
    }
    return values;
}
bool ScopeVar::ScopeExist() { return !scopes_.empty(); }

const std::vector<::llvm::Value*>* ScopeVar::GetScopeIteratorValues() {
    if (scopes_.size() <= 0) {
        LOG(WARNING) << "no scope exists ";
        return nullptr;
    }
    Scope& exist_scope = scopes_.back();
    return &(exist_scope.scope_iterators);
}

}  // namespace codegen
}  // namespace fesql
