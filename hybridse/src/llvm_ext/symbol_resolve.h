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
#ifndef HYBRIDSE_SRC_LLVM_EXT_SYMBOL_RESOLVE_H_
#define HYBRIDSE_SRC_LLVM_EXT_SYMBOL_RESOLVE_H_

#include <map>
#include <string>
#include <utility>
#include "glog/logging.h"
#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/IRTransformLayer.h"
#include "llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/Transforms/Utils.h"

namespace hybridse {
namespace vm {

class HybridSeSymbolResolver : public ::llvm::LegacyJITSymbolResolver {
 public:
    explicit HybridSeSymbolResolver(const ::llvm::DataLayout& data_layout);
    ::llvm::JITSymbol findSymbol(const std::string& Name) override;
    ::llvm::JITSymbol findSymbolInLogicalDylib(const std::string& Name);
    void addSymbol(const std::string& name, void* addr);

 private:
    ::llvm::DataLayout data_layout_;
    std::map<std::string, void*> symbol_dict_;
};

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_LLVM_EXT_SYMBOL_RESOLVE_H_
