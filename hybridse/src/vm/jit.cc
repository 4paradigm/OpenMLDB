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

#include "vm/jit.h"

#include <string>
#include <utility>
extern "C" {
#include <cmath>
#include <cstdlib>
}

#include "absl/cleanup/cleanup.h"
#include "absl/time/clock.h"
#include "glog/logging.h"
#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/IRTransformLayer.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/Transforms/Utils.h"
#ifdef LLVM_EXT_ENABLE
#include "llvm_ext/symbol_resolve.h"
#endif

namespace hybridse {
namespace vm {
using ::llvm::orc::LLJIT;

HybridSeJit::HybridSeJit(::llvm::orc::LLJITBuilderState& s, ::llvm::Error& e)
    : LLJIT(s, e) {}
HybridSeJit::~HybridSeJit() {}

static void RunDefaultOptPasses(::llvm::Module* m) {
    ::llvm::legacy::FunctionPassManager fpm(m);
    // Add some optimizations.
    fpm.add(::llvm::createInstructionCombiningPass());
    fpm.add(::llvm::createReassociatePass());
    fpm.add(::llvm::createGVNPass());
    fpm.add(::llvm::createCFGSimplificationPass());
    fpm.add(::llvm::createPromoteMemoryToRegisterPass());
    fpm.doInitialization();
    for (auto it = m->begin(); it != m->end(); ++it) {
        fpm.run(*it);
    }
}

::llvm::Error HybridSeJit::AddIRModule(::llvm::orc::JITDylib& jd,  // NOLINT
                                       ::llvm::orc::ThreadSafeModule tsm,
                                       ::llvm::orc::VModuleKey key) {
    if (auto err = applyDataLayout(*tsm.getModule())) return err;
    DLOG(INFO) << "add a module with key " << key << " with ins cnt "
               << tsm.getModule()->getInstructionCount();
    RunDefaultOptPasses(tsm.getModule());
    DLOG(INFO) << "after opt with ins cnt "
               << tsm.getModule()->getInstructionCount();
    return CompileLayer->add(jd, std::move(tsm), key);
}

bool HybridSeJit::OptModule(::llvm::Module* m) {
    if (auto err = applyDataLayout(*m)) {
        return false;
    }
    DLOG(INFO) << "Module before opt:\n" << LlvmToString(*m);
    RunDefaultOptPasses(m);
    DLOG(INFO) << "Module after opt:\n" << LlvmToString(*m);
    return true;
}

::llvm::orc::VModuleKey HybridSeJit::CreateVModule() {
    ::llvm::orc::VModuleKey key = ES->allocateVModule();
    DLOG(INFO) << "allocate a new module key " << key;
    return key;
}

void HybridSeJit::ReleaseVModule(::llvm::orc::VModuleKey key) {
    DLOG(INFO) << "release module with key " << key;
    ES->releaseVModule(key);
}

bool HybridSeJit::AddSymbol(::llvm::orc::JITDylib& jd, const std::string& name,
                            void* fn_ptr) {
    if (fn_ptr == NULL) {
        LOG(WARNING) << "fn ptr is null";
        return false;
    }
    ::llvm::orc::MangleAndInterner mi(getExecutionSession(), getDataLayout());
    return HybridSeJit::AddSymbol(jd, mi, name, fn_ptr);
}

bool HybridSeJit::AddSymbol(const std::string& name, void* fn_ptr) {
    if (fn_ptr == NULL) {
        LOG(WARNING) << "fn ptr is null";
        return false;
    }
    auto& jd = getMainJITDylib();
    return AddSymbol(jd, name, fn_ptr);
}

void HybridSeJit::Init() {
    auto& jd = getMainJITDylib();
    auto gen = llvm::orc::DynamicLibrarySearchGenerator::GetForCurrentProcess(
        getDataLayout().getGlobalPrefix());
    auto err = gen.takeError();
    if (err) {
        LOG(WARNING) << "Create process sym failed";
        ::llvm::errs() << err;
        return;
    }
    jd.setGenerator(gen.get());
}

bool HybridSeJit::AddSymbol(::llvm::orc::JITDylib& jd,
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
        return true;
    }
}

bool HybridSeLlvmJitWrapper::Init() {
    absl::Time begin = absl::Now();
    absl::Cleanup clean = [&]() { DLOG(INFO) << "LLVM JIT initialize takes " << absl::Now() - begin; };

    if (initialized_) {
        return true;
    }

    auto jit = ::llvm::Expected<std::unique_ptr<HybridSeJit>>(
        HybridSeJitBuilder().create());
    {
        ::llvm::Error e = jit.takeError();
        if (e) {
            LOG(WARNING) << "fail to init jit let";
            ::llvm::errs() << e;
            return false;
        }
    }
    this->jit_ = std::move(jit.get());
    jit_->Init();

    this->mi_ = std::unique_ptr<::llvm::orc::MangleAndInterner>(
        new ::llvm::orc::MangleAndInterner(jit_->getExecutionSession(),
                                           jit_->getDataLayout()));

    auto s = InitJitSymbols();
    if (!s.isOK()) {
        LOG(WARNING) << s;
        return false;
    }

    initialized_ = true;
    return true;
}

bool HybridSeLlvmJitWrapper::OptModule(::llvm::Module* module) {
    EnsureInitialized();
    return jit_->OptModule(module);
}

bool HybridSeLlvmJitWrapper::AddModule(
    std::unique_ptr<llvm::Module> module,
    std::unique_ptr<llvm::LLVMContext> llvm_ctx) {
    EnsureInitialized();

    ::llvm::Error e = jit_->addIRModule(
        ::llvm::orc::ThreadSafeModule(std::move(module), std::move(llvm_ctx)));
    if (e) {
        LOG(WARNING) << "fail to add ir module: " << LlvmToString(e);
        return false;
    }
    return true;
}

RawPtrHandle HybridSeLlvmJitWrapper::FindFunction(const std::string& funcname) {
    if (funcname == "") {
        return 0;
    }
    ::llvm::Expected<::llvm::JITEvaluatedSymbol> symbol(jit_->lookup(funcname));
    ::llvm::Error e = symbol.takeError();
    if (e) {
        LOG(WARNING) << "fail to resolve fn address of " << funcname << ": "
                     << LlvmToString(e);
        return 0;
    }
    return reinterpret_cast<const int8_t*>(symbol->getAddress());
}

bool HybridSeLlvmJitWrapper::AddExternalFunction(const std::string& name,
                                               void* addr) {
    return hybridse::vm::HybridSeJit::AddSymbol(jit_->getMainJITDylib(), *mi_,
                                                name, addr);
}

#ifdef LLVM_EXT_ENABLE
bool HybridSeMcJitWrapper::Init() {
    auto s = InitJitSymbols();
    if (!s.isOK()) {
        LOG(WARNING) << s;
        return false;
    }
    return true;
}

bool HybridSeMcJitWrapper::OptModule(::llvm::Module* module) {
    EnsureInitialized();

    DLOG(INFO) << "Module before opt:\n" << LlvmToString(*module);
    RunDefaultOptPasses(module);
    DLOG(INFO) << "Module after opt:\n" << LlvmToString(*module);
    return true;
}

bool HybridSeMcJitWrapper::AddModule(
    std::unique_ptr<llvm::Module> module,
    std::unique_ptr<llvm::LLVMContext> llvm_ctx) {
    EnsureInitialized();

    if (llvm::verifyModule(*module, &llvm::errs(), nullptr)) {
        // note: destruct module before ctx
        module = nullptr;
        llvm_ctx = nullptr;
        return false;
    }
    if (execution_engine_ == nullptr) {
        const ::llvm::DataLayout& module_layout = module->getDataLayout();
        llvm::EngineBuilder engine_builder(std::move(module));

        auto resolver = new HybridSeSymbolResolver(
            module_layout.isDefault()
                ? engine_builder.selectTarget()->createDataLayout()
                : module_layout);

        execution_engine_ =
            engine_builder.setEngineKind(llvm::EngineKind::JIT)
                .setErrorStr(&err_str_)
                .setVerifyModules(true)
                .setOptLevel(::llvm::CodeGenOpt::Level::Default)
                .setSymbolResolver(
                    std::unique_ptr<::llvm::LegacyJITSymbolResolver>(
                        ::llvm::cast<::llvm::LegacyJITSymbolResolver>(
                            resolver)))
                .create();
        if (execution_engine_ == nullptr || !err_str_.empty()) {
            LOG(WARNING) << "Create mcjit execution engine failed, "
                         << err_str_;
            return false;
        }
        for (auto& pair : extern_functions_) {
            resolver->addSymbol(pair.first, pair.second);
        }
    } else {
        execution_engine_->addModule(std::move(module));
    }
    if (jit_options_.IsEnableVTune()) {
        auto listener = ::llvm::JITEventListener::createIntelJITEventListener();
        if (listener == nullptr) {
            LOG(WARNING) << "Intel jit events is not enabled";
        } else {
            execution_engine_->RegisterJITEventListener(listener);
        }
    }
    if (jit_options_.IsEnableGDB()) {
        auto listener =
            ::llvm::JITEventListener::createGDBRegistrationListener();
        if (listener == nullptr) {
            LOG(WARNING) << "GDB jit events is not enabled";
        } else {
#if LLVM_VERSION_MAJOR != 9
            // register duplicate gdb listener LLVM 9 is illegal
            // and  mcjit register it automatically in this version.
            // execution_engine_->RegisterJITEventListener(listener);
            LOG(WARNING) << "Use LLVM major version=" << LLVM_VERSION_MAJOR
                         << ", gdb event support may not be automatic";
#endif
        }
    }
    if (jit_options_.IsEnablePerf()) {
        auto listener = ::llvm::JITEventListener::createPerfJITEventListener();
        if (listener == nullptr) {
            LOG(WARNING) << "Perf jit events is not enabled";
        } else {
            execution_engine_->RegisterJITEventListener(listener);
        }
    }
    execution_engine_->finalizeObject();
    return CheckError();
}

bool HybridSeMcJitWrapper::AddExternalFunction(const std::string& name,
                                               void* addr) {
    if (execution_engine_ != nullptr) {
        LOG(WARNING)
            << "Can not register external symbol after engine initialized: "
            << name;
        return false;
    }
    extern_functions_[name] = addr;
    return true;
}

hybridse::vm::RawPtrHandle HybridSeMcJitWrapper::FindFunction(
    const std::string& funcname) {
    if (!CheckInitialized()) {
        return nullptr;
    }
    auto addr = execution_engine_->getFunctionAddress(funcname);
    if (!CheckError()) {
        return nullptr;
    }
    return reinterpret_cast<int8_t*>(addr);
}

bool HybridSeMcJitWrapper::CheckError() {
    if (!err_str_.empty()) {
        LOG(WARNING) << "Detect jit error: " << err_str_;
        err_str_.clear();
        return false;
    }
    return true;
}

bool HybridSeMcJitWrapper::CheckInitialized() const {
    if (execution_engine_ == nullptr) {
        LOG(WARNING) << "JIT engine is not initialized";
        return false;
    }
    return true;
}
#endif

}  // namespace vm
}  // namespace hybridse
