/*
 * Copyright (C) 4Paradigm
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

ScopeVar::ScopeVar() : parent_(nullptr) {}
ScopeVar::ScopeVar(ScopeVar* parent) : parent_(parent) {}
ScopeVar::~ScopeVar() {}

bool ScopeVar::AddIteratorValue(::llvm::Value* value) {
    scope_iterators_.push_back(value);
    return true;
}

bool ScopeVar::AddVar(const std::string& name, const NativeValue& value) {
    std::map<std::string, NativeValue>::iterator it = scope_map_.find(name);
    if (it != scope_map_.end()) {
        LOG(WARNING) << "var with name " << name << " exists ";
        return false;
    }
    scope_map_.insert(it, std::make_pair(name, value));
    DLOG(INFO) << "store var " << name;
    return true;
}

bool ScopeVar::ReplaceVar(const std::string& name, const NativeValue& value) {
    ScopeVar* cur = this;
    while (cur != nullptr) {
        std::map<std::string, NativeValue>::iterator it =
            cur->scope_map_.find(name);
        if (it != cur->scope_map_.end()) {
            it->second = value;
            return true;
        }
        cur = cur->parent();
    }
    DLOG(INFO) << "var with name " << name << " does not exist ";
    return false;
}

bool ScopeVar::FindVar(const std::string& name, NativeValue* value) {
    if (value == NULL) {
        LOG(WARNING) << " input value is null";
        return false;
    }
    ScopeVar* cur = this;
    while (cur != nullptr) {
        std::map<std::string, NativeValue>::iterator it =
            cur->scope_map_.find(name);
        if (it != cur->scope_map_.end()) {
            *value = it->second;
            return true;
        }
        cur = cur->parent();
    }
    DLOG(INFO) << "var with name " << name << " does not exist ";
    return false;
}

bool ScopeVar::HasVar(const std::string& name) {
    ScopeVar* cur = this;
    while (cur != nullptr) {
        std::map<std::string, NativeValue>::iterator it =
            cur->scope_map_.find(name);
        if (it != cur->scope_map_.end()) {
            return true;
        }
        cur = cur->parent();
    }
    return false;
}

std::vector<const std::vector<::llvm::Value*>*> ScopeVar::GetIteratorValues() {
    std::vector<const std::vector<::llvm::Value*>*> values;
    ScopeVar* cur = this;
    while (cur != nullptr) {
        values.push_back(&(cur->scope_iterators_));
        cur = cur->parent();
    }
    return values;
}

const std::vector<::llvm::Value*>* ScopeVar::GetScopeIteratorValues() {
    return &scope_iterators_;
}

}  // namespace codegen
}  // namespace fesql
