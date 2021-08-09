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
#include "llvm_ext/symbol_resolve.h"
#include "llvm/IR/Mangler.h"

namespace hybridse {
namespace vm {

HybridSeSymbolResolver::HybridSeSymbolResolver(
    const ::llvm::DataLayout& data_layout)
    : data_layout_(data_layout) {}

::llvm::JITSymbol HybridSeSymbolResolver::findSymbol(const std::string& name) {
    auto iter = symbol_dict_.find(name);
    if (iter != symbol_dict_.end()) {
        DLOG(INFO) << "Find " << name << ": " << (uint64_t)iter->second;
        return ::llvm::JITSymbol((uint64_t)iter->second,
                                 ::llvm::JITSymbolFlags::Absolute);
    }
    return nullptr;
}

::llvm::JITSymbol HybridSeSymbolResolver::findSymbolInLogicalDylib(
    const std::string& name) {
    return nullptr;
}

void HybridSeSymbolResolver::addSymbol(const std::string& name, void* addr) {
    ::llvm::SmallString<128> mangle_name;
    ::llvm::Mangler::getNameWithPrefix(mangle_name, name, data_layout_);
    DLOG(INFO) << "Add symbol " << name << " -> " << mangle_name.str().str();
    symbol_dict_[mangle_name.str().str()] = addr;
}

}  // namespace vm
}  // namespace hybridse
