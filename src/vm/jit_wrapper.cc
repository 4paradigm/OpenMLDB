/*
 * jit_wrapper.cc
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
#include <string>
#include <utility>

#include "glog/logging.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/jit.h"
#include "vm/jit_wrapper.h"
#include "vm/sql_compiler.h"

namespace fesql {
namespace vm {

bool FeSQLJITWrapper::Init() {
    DLOG(INFO) << "Start to initialize fesql jit";
    auto jit =
        ::llvm::Expected<std::unique_ptr<FeSQLJIT>>(FeSQLJITBuilder().create());
    {
        ::llvm::Error e = jit.takeError();
        if (e) {
            LOG(WARNING) << "fail to init jit let";
            return false;
        }
    }
    this->jit_ = std::move(jit.get());
    jit_->Init();
    return true;
}

bool FeSQLJITWrapper::AddModule(std::unique_ptr<llvm::Module> module,
                                std::unique_ptr<llvm::LLVMContext> llvm_ctx) {
    ::llvm::Error e = jit_->addIRModule(
        ::llvm::orc::ThreadSafeModule(std::move(module), std::move(llvm_ctx)));
    if (e) {
        std::string err_str;
        ::llvm::raw_string_ostream ss(err_str);
        ss << e;
        LOG(WARNING) << "fail to add ir module: " << err_str;
        return false;
    }
    InitCodecSymbol(jit_.get());
    udf::InitUDFSymbol(jit_.get());
    udf::DefaultUDFLibrary* library = udf::DefaultUDFLibrary::get();
    library->InitJITSymbols(jit_.get());
    return true;
}

bool FeSQLJITWrapper::AddModuleFromBuffer(const base::RawBuffer& buf) {
    std::string buf_str(buf.addr, buf.size);
    ::llvm::SMDiagnostic diagnostic;
    auto llvm_ctx = ::llvm::make_unique<::llvm::LLVMContext>();
    auto mem_buf = ::llvm::MemoryBuffer::getMemBuffer(buf_str);
    auto llvm_module = parseIR(*mem_buf, diagnostic, *llvm_ctx);
    if (llvm_module == nullptr) {
        LOG(WARNING) << "Parse module failed: module string is\n" << buf_str;
        std::string err_msg;
        llvm::raw_string_ostream err_msg_stream(err_msg);
        diagnostic.print("", err_msg_stream);
        return false;
    }
    return this->AddModule(std::move(llvm_module), std::move(llvm_ctx));
}

RawPtrHandle FeSQLJITWrapper::FindFunction(const std::string& funcname) {
    if (funcname == "") {
        return 0;
    }
    ::llvm::Expected<::llvm::JITEvaluatedSymbol> symbol(jit_->lookup(funcname));
    if (symbol.takeError()) {
        LOG(WARNING) << "fail to resolve fn address of" << funcname;
        return 0;
    }
    return reinterpret_cast<const int8_t*>(symbol->getAddress());
}

}  // namespace vm
}  // namespace fesql
