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
#include "vm/jit_wrapper.h"

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
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/Transforms/Utils.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/jit.h"

namespace hybridse {
namespace vm {

bool HybridSeJitWrapper::AddModuleFromBuffer(const base::RawBuffer& buf) {
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

bool HybridSeJitWrapper::InitJitSymbols(HybridSeJitWrapper* jit) {
    InitBuiltinJitSymbols(jit);
    udf::DefaultUdfLibrary::get()->InitJITSymbols(jit);
    return true;
}

HybridSeJitWrapper* HybridSeJitWrapper::Create() {
    return Create(JitOptions());
}

HybridSeJitWrapper* HybridSeJitWrapper::Create(const JitOptions& jit_options) {
    if (jit_options.IsEnableMcjit()) {
#ifdef LLVM_EXT_ENABLE
        LOG(INFO) << "Create McJit engine";
        return new HybridSeMcJitWrapper(jit_options);
#else
        LOG(WARNING) << "McJit support is not enabled";
        return new HybridSeLlvmJitWrapper();
#endif
    } else {
        if (jit_options.IsEnableVtune() || jit_options.IsEnablePerf() ||
            jit_options.IsEnableGdb()) {
            LOG(WARNING) << "LLJIT do not support jit events";
        }
        return new HybridSeLlvmJitWrapper();
    }
}

void HybridSeJitWrapper::DeleteJit(HybridSeJitWrapper* jit) {
    if (jit != nullptr) {
        delete jit;
    }
}

void InitBuiltinJitSymbols(HybridSeJitWrapper* jit) {
    jit->AddExternalFunction("malloc", (reinterpret_cast<void*>(&malloc)));
    jit->AddExternalFunction("memset", (reinterpret_cast<void*>(&memset)));
    jit->AddExternalFunction("memcpy", (reinterpret_cast<void*>(&memcpy)));
    jit->AddExternalFunction("__bzero", (reinterpret_cast<void*>(&bzero)));

    jit->AddExternalFunction(
        "hybridse_storage_get_bool_field",
        reinterpret_cast<void*>(
            static_cast<int8_t (*)(const int8_t*, uint32_t, uint32_t, int8_t*)>(
                &codec::v1::GetBoolField)));
    jit->AddExternalFunction(
        "hybridse_storage_get_int16_field",
        reinterpret_cast<void*>(
            static_cast<int16_t (*)(const int8_t*, uint32_t, uint32_t,
                                    int8_t*)>(&codec::v1::GetInt16Field)));
    jit->AddExternalFunction(
        "hybridse_storage_get_int32_field",
        reinterpret_cast<void*>(
            static_cast<int32_t (*)(const int8_t*, uint32_t, uint32_t,
                                    int8_t*)>(&codec::v1::GetInt32Field)));
    jit->AddExternalFunction(
        "hybridse_storage_get_int64_field",
        reinterpret_cast<void*>(
            static_cast<int64_t (*)(const int8_t*, uint32_t, uint32_t,
                                    int8_t*)>(&codec::v1::GetInt64Field)));
    jit->AddExternalFunction(
        "hybridse_storage_get_float_field",
        reinterpret_cast<void*>(
            static_cast<float (*)(const int8_t*, uint32_t, uint32_t, int8_t*)>(
                &codec::v1::GetFloatField)));
    jit->AddExternalFunction(
        "hybridse_storage_get_double_field",
        reinterpret_cast<void*>(
            static_cast<double (*)(const int8_t*, uint32_t, uint32_t, int8_t*)>(
                &codec::v1::GetDoubleField)));
    jit->AddExternalFunction(
        "hybridse_storage_get_timestamp_field",
        reinterpret_cast<void*>(&codec::v1::GetTimestampField));
    jit->AddExternalFunction("hybridse_storage_get_str_addr_space",
                             reinterpret_cast<void*>(&codec::v1::GetAddrSpace));
    jit->AddExternalFunction(
        "hybridse_storage_get_str_field",
        reinterpret_cast<void*>(
            static_cast<int32_t (*)(const int8_t*, uint32_t, uint32_t, uint32_t,
                                    uint32_t, uint32_t, const char**, uint32_t*,
                                    int8_t*)>(&codec::v1::GetStrField)));
    jit->AddExternalFunction("hybridse_storage_get_col",
                             reinterpret_cast<void*>(&codec::v1::GetCol));
    jit->AddExternalFunction("hybridse_storage_get_str_col",
                             reinterpret_cast<void*>(&codec::v1::GetStrCol));

    jit->AddExternalFunction(
        "hybridse_storage_get_inner_range_list",
        reinterpret_cast<void*>(&codec::v1::GetInnerRangeList));
    jit->AddExternalFunction(
        "hybridse_storage_get_inner_rows_list",
        reinterpret_cast<void*>(&codec::v1::GetInnerRowsList));
    jit->AddExternalFunction(
        "hybridse_storage_get_inner_rows_range_list",
        reinterpret_cast<void*>(&codec::v1::GetInnerRowsRangeList));

    // encode
    jit->AddExternalFunction("hybridse_storage_encode_int16_field",
                             reinterpret_cast<void*>(&codec::v1::AppendInt16));

    jit->AddExternalFunction("hybridse_storage_encode_int32_field",
                             reinterpret_cast<void*>(&codec::v1::AppendInt32));

    jit->AddExternalFunction("hybridse_storage_encode_int64_field",
                             reinterpret_cast<void*>(&codec::v1::AppendInt64));

    jit->AddExternalFunction("hybridse_storage_encode_float_field",
                             reinterpret_cast<void*>(&codec::v1::AppendFloat));

    jit->AddExternalFunction("hybridse_storage_encode_double_field",
                             reinterpret_cast<void*>(&codec::v1::AppendDouble));

    jit->AddExternalFunction("hybridse_storage_encode_string_field",
                             reinterpret_cast<void*>(&codec::v1::AppendString));
    jit->AddExternalFunction(
        "hybridse_storage_encode_calc_size",
        reinterpret_cast<void*>(&codec::v1::CalcTotalLength));
    jit->AddExternalFunction(
        "hybridse_storage_encode_nullbit",
        reinterpret_cast<void*>(&codec::v1::AppendNullBit));

    // row iteration
    jit->AddExternalFunction(
        "hybridse_storage_get_row_iter",
        reinterpret_cast<void*>(&hybridse::vm::GetRowIter));
    jit->AddExternalFunction(
        "hybridse_storage_row_iter_has_next",
        reinterpret_cast<void*>(&hybridse::vm::RowIterHasNext));
    jit->AddExternalFunction(
        "hybridse_storage_row_iter_next",
        reinterpret_cast<void*>(&hybridse::vm::RowIterNext));
    jit->AddExternalFunction(
        "hybridse_storage_row_iter_get_cur_slice",
        reinterpret_cast<void*>(&hybridse::vm::RowIterGetCurSlice));
    jit->AddExternalFunction(
        "hybridse_storage_row_iter_get_cur_slice_size",
        reinterpret_cast<void*>(&hybridse::vm::RowIterGetCurSliceSize));

    jit->AddExternalFunction(
        "hybridse_storage_row_iter_delete",
        reinterpret_cast<void*>(&hybridse::vm::RowIterDelete));
    jit->AddExternalFunction(
        "hybridse_storage_get_row_slice",
        reinterpret_cast<void*>(&hybridse::vm::RowGetSlice));
    jit->AddExternalFunction(
        "hybridse_storage_get_row_slice_size",
        reinterpret_cast<void*>(&hybridse::vm::RowGetSliceSize));

    jit->AddExternalFunction(
        "hybridse_memery_pool_alloc",
        reinterpret_cast<void*>(&udf::v1::AllocManagedStringBuf));

    jit->AddExternalFunction(
        "fmod", reinterpret_cast<void*>(
                    static_cast<double (*)(double, double)>(&fmod)));
    jit->AddExternalFunction("fmodf", reinterpret_cast<void*>(&fmodf));
}

}  // namespace vm
}  // namespace hybridse
