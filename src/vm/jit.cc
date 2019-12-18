/*
 * jit.cc
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

#include "vm/jit.h"

#include <string>
#include <utility>
extern "C" {
#include <cstdlib>
}
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
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/Transforms/Utils.h"

namespace fesql {
namespace vm {
using ::llvm::orc::LLJIT;

FeSQLJIT::FeSQLJIT(::llvm::orc::LLJITBuilderState& s, ::llvm::Error& e)
    : LLJIT(s, e) {}
FeSQLJIT::~FeSQLJIT() {}

::llvm::Error FeSQLJIT::AddIRModule(::llvm::orc::JITDylib& jd,  // NOLINT
                                    ::llvm::orc::ThreadSafeModule tsm,
                                    ::llvm::orc::VModuleKey key) {
    if (auto err = applyDataLayout(*tsm.getModule())) return err;
    LOG(INFO) << "add a module with key " << key << " with ins cnt "
              << tsm.getModule()->getInstructionCount();
    ::llvm::legacy::FunctionPassManager fpm(tsm.getModule());
    // Add some optimizations.
    fpm.add(::llvm::createInstructionCombiningPass());
    fpm.add(::llvm::createReassociatePass());
    fpm.add(::llvm::createGVNPass());
    fpm.add(::llvm::createCFGSimplificationPass());
    fpm.doInitialization();
    ::llvm::Module::iterator it;
    ::llvm::Module::iterator end = tsm.getModule()->end();
    for (it = tsm.getModule()->begin(); it != end; ++it) {
        fpm.run(*it);
    }
    LOG(INFO) << "after opt with ins cnt "
              << tsm.getModule()->getInstructionCount();
    return CompileLayer->add(jd, std::move(tsm), key);
}

::llvm::Error FeSQLJIT::OptModule(::llvm::Module* m) {
    if (auto err = applyDataLayout(*m)) {
        return err;
    }
    LOG(INFO) << "before opt with ins cnt " << m->getInstructionCount();
    ::llvm::legacy::FunctionPassManager fpm(m);
    fpm.add(::llvm::createPromoteMemoryToRegisterPass());
    // Add some optimizations.
    fpm.add(::llvm::createInstructionCombiningPass());
    fpm.add(::llvm::createReassociatePass());
    fpm.add(::llvm::createGVNPass());
    fpm.add(::llvm::createCFGSimplificationPass());
    fpm.doInitialization();
    ::llvm::Module::iterator it;
    ::llvm::Module::iterator end = m->end();
    for (it = m->begin(); it != end; ++it) {
        fpm.run(*it);
    }
    LOG(INFO) << "after opt with ins cnt " << m->getInstructionCount();
}

::llvm::orc::VModuleKey FeSQLJIT::CreateVModule() {
    ::llvm::orc::VModuleKey key = ES->allocateVModule();
    LOG(INFO) << "allocate a new module key " << key;
    return key;
}

void FeSQLJIT::ReleaseVModule(::llvm::orc::VModuleKey key) {
    LOG(INFO) << "release module with key " << key;
    ES->releaseVModule(key);
}

bool FeSQLJIT::AddSymbol(::llvm::orc::JITDylib& jd, const std::string& name,
                         void* fn_ptr) {
    if (fn_ptr == NULL) {
        LOG(WARNING) << "fn ptr is null";
        return false;
    }
    ::llvm::orc::MangleAndInterner mi(getExecutionSession(), getDataLayout());
    return FeSQLJIT::AddSymbol(jd, mi, name, fn_ptr);
}

bool FeSQLJIT::AddSymbol(const std::string& name, void* fn_ptr) {
    if (fn_ptr == NULL) {
        LOG(WARNING) << "fn ptr is null";
        return false;
    }
    auto& jd = getMainJITDylib();
    return AddSymbol(jd, name, fn_ptr);
}

void FeSQLJIT::Init() { AddSymbol("malloc", reinterpret_cast<void*>(&malloc)); }
bool FeSQLJIT::AddSymbol(::llvm::orc::JITDylib& jd,
                         ::llvm::orc::MangleAndInterner& mi,
                         const std::string& fn_name, void* fn_ptr) {
    ::llvm::StringRef symbol(fn_name);
    ::llvm::JITEvaluatedSymbol jit_symbol(
        ::llvm::pointerToJITTargetAddress(fn_ptr), ::llvm::JITSymbolFlags());
    ::llvm::orc::SymbolMap symbol_map;
    symbol_map.insert(std::make_pair(mi(symbol), jit_symbol));
    auto err = jd.define(::llvm::orc::absoluteSymbols(symbol_map));
    if (err) {
        LOG(WARNING) << "fail to add symbol " << fn_name;
        return false;
    } else {
        LOG(INFO) << "add fn symbol " << fn_name << " done";
        return true;
    }
}

}  // namespace vm
}  // namespace fesql
